// Package redisbus keeps go-store caches consistent across processes by
// broadcasting invalidations over Redis pub/sub.
//
// Each process creates one Bus on a shared channel and binds its caches under
// namespaces. Invalidating a key through a binding deletes it locally and
// publishes a message; every other process deletes the key from its cache
// bound under the same namespace. Processes that write data but hold no
// cache, such as an admin service, call Publish directly.
//
// Pub/sub delivers at most once: messages sent while a subscriber is
// disconnected are lost. A Bus therefore clears every bound cache whenever
// its subscription is (re)established, and cache TTLs remain the last line of
// defense. Only PUBLISH and SUBSCRIBE are used, so the package works with
// Redis Cluster and ElastiCache Serverless.
package redisbus

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-redis/redis/v8"
)

// receiveBuffer is large so that a burst of invalidations is not dropped by
// go-redis, which discards messages when a subscriber falls behind.
const receiveBuffer = 1024

// Bus publishes and receives cache invalidations on one Redis channel.
type Bus struct {
	client  redis.UniversalClient
	channel string
	id      string

	mu       sync.Mutex
	handlers map[string]handler // guarded by mu
	pubsub   *redis.PubSub      // guarded by mu; nil until the first bind
	closed   bool               // guarded by mu
	done     chan struct{}

	published, received, resets, dropped atomic.Uint64
}

type handler struct {
	keys func([]string) error // invalidate these keys; error if any is undecodable
	all  func()               // invalidate everything
}

type message struct {
	From string   `json:"from"`
	NS   string   `json:"ns"`
	Keys []string `json:"keys,omitempty"`
	All  bool     `json:"all,omitempty"`
}

// Stats counts a Bus's activity.
type Stats struct {
	// Published and Received count messages sent and acted on.
	Published, Received uint64
	// Resets counts subscriptions (re)established, each of which cleared
	// every bound cache.
	Resets uint64
	// Dropped counts received messages that were malformed, named an
	// unbound namespace, or carried undecodable keys.
	Dropped uint64
}

// New returns a Bus on channel. It subscribes when the first cache is bound;
// a Bus used only to Publish never subscribes.
func New(client redis.UniversalClient, channel string) (*Bus, error) {
	if client == nil {
		return nil, errors.New("redisbus: client cannot be nil")
	}
	if channel == "" {
		return nil, errors.New("redisbus: channel cannot be empty")
	}
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("redisbus: instance id: %w", err)
	}
	return &Bus{
		client: client, channel: channel, id: hex.EncodeToString(id),
		handlers: make(map[string]handler),
	}, nil
}

// Publish tells other processes to invalidate keys in namespace. Keys use the
// binding's encoding: the string itself, or an integer in base 10.
func (b *Bus) Publish(ctx context.Context, namespace string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return b.publish(ctx, message{NS: namespace, Keys: keys})
}

// PublishAll tells other processes to invalidate everything in namespace.
func (b *Bus) PublishAll(ctx context.Context, namespace string) error {
	return b.publish(ctx, message{NS: namespace, All: true})
}

func (b *Bus) publish(ctx context.Context, m message) error {
	m.From = b.id
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := b.client.Publish(ctx, b.channel, payload).Err(); err != nil {
		return fmt.Errorf("redisbus: publish %s: %w", m.NS, err)
	}
	b.published.Add(1)
	return nil
}

func (b *Bus) Stats() Stats {
	return Stats{
		Published: b.published.Load(), Received: b.received.Load(),
		Resets: b.resets.Load(), Dropped: b.dropped.Load(),
	}
}

// Close stops receiving invalidations. Bound caches keep their contents.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	ps := b.pubsub
	b.mu.Unlock()
	if ps == nil {
		return nil
	}
	err := ps.Close()
	<-b.done
	return err
}

func (b *Bus) bind(namespace string, h handler) error {
	if namespace == "" {
		return errors.New("redisbus: namespace cannot be empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("redisbus: bus is closed")
	}
	if _, ok := b.handlers[namespace]; ok {
		return fmt.Errorf("redisbus: namespace %q is already bound", namespace)
	}
	b.handlers[namespace] = h
	if b.pubsub == nil {
		ctx := context.Background()
		b.pubsub = b.client.Subscribe(ctx, b.channel)
		b.done = make(chan struct{})
		go b.receive(b.pubsub.ChannelWithSubscriptions(ctx, receiveBuffer))
	}
	return nil
}

func (b *Bus) receive(ch <-chan interface{}) {
	defer close(b.done)
	for msg := range ch {
		switch msg := msg.(type) {
		case *redis.Subscription:
			if msg.Kind == "subscribe" {
				b.reset()
			}
		case *redis.Message:
			b.handle(msg.Payload)
		}
	}
}

// reset clears every bound cache, since invalidations may have been missed
// while the subscription was down.
func (b *Bus) reset() {
	b.resets.Add(1)
	for _, h := range b.snapshotHandlers() {
		h.all()
	}
}

func (b *Bus) handle(payload string) {
	var m message
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		b.dropped.Add(1)
		return
	}
	if m.From == b.id {
		return // already applied locally before publishing
	}
	b.mu.Lock()
	h, ok := b.handlers[m.NS]
	b.mu.Unlock()
	if !ok {
		b.dropped.Add(1)
		return
	}
	b.received.Add(1)
	if m.All {
		h.all()
	} else if err := h.keys(m.Keys); err != nil {
		b.dropped.Add(1)
	}
}

func (b *Bus) snapshotHandlers() []handler {
	b.mu.Lock()
	defer b.mu.Unlock()
	hs := make([]handler, 0, len(b.handlers))
	for _, h := range b.handlers {
		hs = append(hs, h)
	}
	return hs
}
