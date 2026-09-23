package store

import (
	"fmt"
	"time"
)

const defaultShards = 64

// EvictionReason explains why an entry left the store without an explicit
// Delete call.
type EvictionReason uint8

const (
	Expired EvictionReason = iota + 1
	Capacity
)

// Option configures a Store.
type Option[K comparable, V any] func(*config[K, V]) error

type config[K comparable, V any] struct {
	shards          int
	maxCost         int64
	cost            func(K, V) int64
	cleanupInterval time.Duration
	onEvict         func(K, V, EvictionReason)
}

// WithShards sets the number of independently locked shards. n must be a
// power of two.
func WithShards[K comparable, V any](n int) Option[K, V] {
	return func(c *config[K, V]) error {
		if n <= 0 || n&(n-1) != 0 {
			return fmt.Errorf("store: shard count must be a positive power of two")
		}
		c.shards = n
		return nil
	}
}

// WithMaxCost bounds the sum of entry costs. A zero limit means unbounded.
// Entries with a cost larger than maxCost are rejected.
func WithMaxCost[K comparable, V any](maxCost int64, cost func(K, V) int64) Option[K, V] {
	return func(c *config[K, V]) error {
		if maxCost < 0 {
			return fmt.Errorf("store: max cost cannot be negative")
		}
		if maxCost > 0 && cost == nil {
			return fmt.Errorf("store: a cost function is required with max cost")
		}
		c.maxCost, c.cost = maxCost, cost
		return nil
	}
}

// WithCleanupInterval enables periodic removal of expired entries. Expired
// entries are always hidden and lazily removed even when cleanup is disabled.
func WithCleanupInterval[K comparable, V any](interval time.Duration) Option[K, V] {
	return func(c *config[K, V]) error {
		if interval < 0 {
			return fmt.Errorf("store: cleanup interval cannot be negative")
		}
		c.cleanupInterval = interval
		return nil
	}
}

// WithEvictionCallback registers a synchronous callback for expiry and
// capacity evictions. It is not called for Delete or replacement.
func WithEvictionCallback[K comparable, V any](fn func(K, V, EvictionReason)) Option[K, V] {
	return func(c *config[K, V]) error {
		c.onEvict = fn
		return nil
	}
}
