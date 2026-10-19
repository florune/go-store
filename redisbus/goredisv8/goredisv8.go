// Package goredisv8 runs a redisbus.Bus on a go-redis v8 client
// (github.com/go-redis/redis/v8).
package goredisv8

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"

	"github.com/florune/go-store/redisbus"
)

// receiveBuffer is large so that a burst of invalidations is not dropped by
// go-redis, which discards messages when a subscriber falls behind.
const receiveBuffer = 1024

// New returns a Bus on channel that publishes and subscribes through client:
// a *redis.Client, *redis.ClusterClient, or any other UniversalClient. It
// subscribes when the first cache is bound; a Bus used only to Publish never
// subscribes.
func New(client redis.UniversalClient, channel string) (*redisbus.Bus, error) {
	if client == nil {
		return nil, errors.New("goredisv8: client cannot be nil")
	}
	return redisbus.New(conn{client}, channel)
}

type conn struct{ client redis.UniversalClient }

func (c conn) Publish(ctx context.Context, channel, payload string) error {
	return c.client.Publish(ctx, channel, payload).Err()
}

func (c conn) Subscribe(channel string, subscribed func(), message func(string)) func() error {
	ctx := context.Background()
	ps := c.client.Subscribe(ctx, channel)
	ch := ps.ChannelWithSubscriptions(ctx, receiveBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range ch {
			switch msg := msg.(type) {
			case *redis.Subscription:
				if msg.Kind == "subscribe" {
					subscribed()
				}
			case *redis.Message:
				message(msg.Payload)
			}
		}
	}()
	return func() error {
		err := ps.Close()
		<-done
		return err
	}
}
