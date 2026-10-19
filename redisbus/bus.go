// Package redisbus keeps go-store caches consistent across processes by
// broadcasting invalidations over Redis pub/sub.
//
// Each process creates one Bus on a shared channel and binds its caches under
// namespaces. Create it with the subpackage for your client, goredisv9.New or
// goredisv8.New; New takes any other client through Conn. Invalidating a key
// through a binding deletes it locally and publishes a message; every other
// process deletes the key from its cache bound under the same namespace.
// Processes that write data but hold no cache, such as an admin service, call
// Publish directly.
//
// Pub/sub delivers at most once: messages sent while a subscriber is
// disconnected are lost. A Bus therefore clears every bound cache whenever
// its subscription is (re)established, and cache TTLs remain the last line of
// defense. Only PUBLISH and SUBSCRIBE are used, so the package works with
// Redis Cluster and ElastiCache Serverless. Messages have the same format for
// every client, so processes on different go-redis versions share a channel.
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
)

// Conn is the Redis connection a Bus runs on. Packages goredisv9 and
// goredisv8 implement it for go-redis; implement it only for another client.
type Conn interface {
	// Publish sends payload on channel.
	Publish(ctx context.Context, channel, payload string) error
	// Subscribe subscribes to channel and, on a goroutine of its own, calls
	// subscribed each time the subscription is established or re-established
	// and message for each message received, one call at a time. Calling the
	// returned stop ends the subscription; stop returns after the last call.
	Subscribe(channel string, subscribed func(), message func(payload string)) (stop func() error)
}

// Bus publishes and receives cache invalidations on one Redis channel.
type Bus struct {
	conn    Conn
	channel string
	id      string

	mu       sync.Mutex
	handlers map[string]handler // guarded by mu
	stop     func() error       // guarded by mu; nil until the first bind
	closed   bool               // guarded by mu

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
func New(conn Conn, channel string) (*Bus, error) {
	if conn == nil {
		return nil, errors.New("redisbus: conn cannot be nil")
	}
	if channel == "" {
		return nil, errors.New("redisbus: channel cannot be empty")
	}
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("redisbus: instance id: %w", err)
	}
	return &Bus{
		conn: conn, channel: channel, id: hex.EncodeToString(id),
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
	if err := b.conn.Publish(ctx, b.channel, string(payload)); err != nil {
		return fmt.Errorf("redisbus: publish %s: %w", m.NS, err)
	}
	b.published.Add(1)
	return nil
}

// Stats returns the Bus's current counters.
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
	stop := b.stop
	b.mu.Unlock()
	if stop == nil {
		return nil
	}
	return stop()
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
	if b.stop == nil {
		b.stop = b.conn.Subscribe(b.channel, b.reset, b.handle)
	}
	return nil
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
