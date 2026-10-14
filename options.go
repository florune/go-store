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
	// Expired means the entry's TTL elapsed.
	Expired EvictionReason = iota + 1
	// Capacity means the entry was evicted to stay within WithMaxCost.
	Capacity
)

// Option configures a Store.
type Option interface {
	applyStore(*storeConfig) error
}

// SnapshotOption configures a Snapshot.
type SnapshotOption interface {
	applySnapshot(*snapshotConfig) error
}

// SharedOption configures either a Store or a Snapshot.
type SharedOption interface {
	Option
	SnapshotOption
}

type storeConfig struct {
	shards          int
	maxCost         int64
	cost            any // func(K, V) int64, checked by New
	onEvict         any // func(K, V, EvictionReason), checked by New
	cleanupInterval time.Duration
	name            string
	jitter          float64
	negativeTTL     time.Duration
	loadTimeout     time.Duration
	staleGrace      time.Duration
	refreshAhead    time.Duration
}

type storeOption func(*storeConfig) error

func (f storeOption) applyStore(c *storeConfig) error { return f(c) }

type sharedOption struct {
	store    func(*storeConfig) error
	snapshot func(*snapshotConfig) error
}

func (o sharedOption) applyStore(c *storeConfig) error       { return o.store(c) }
func (o sharedOption) applySnapshot(c *snapshotConfig) error { return o.snapshot(c) }

func nonNegative(what string, d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("store: %s cannot be negative", what)
	}
	return nil
}

// WithName names a Store or Snapshot for AllStats and PublishExpvar. Names
// must be unique among open stores and snapshots; Close releases the name.
func WithName(name string) SharedOption {
	check := func() error {
		if name == "" {
			return fmt.Errorf("store: name cannot be empty")
		}
		return nil
	}
	return sharedOption{
		store:    func(c *storeConfig) error { c.name = name; return check() },
		snapshot: func(c *snapshotConfig) error { c.name = name; return check() },
	}
}

// WithLoadTimeout bounds each load: a GetOrLoad Loader call, or a Snapshot
// load or version check.
func WithLoadTimeout(timeout time.Duration) SharedOption {
	return sharedOption{
		store: func(c *storeConfig) error {
			c.loadTimeout = timeout
			return nonNegative("load timeout", timeout)
		},
		snapshot: func(c *snapshotConfig) error {
			c.loadTimeout = timeout
			return nonNegative("load timeout", timeout)
		},
	}
}

// WithShards sets the number of independently locked shards. n must be a
// power of two.
func WithShards(n int) Option {
	return storeOption(func(c *storeConfig) error {
		if n <= 0 || n&(n-1) != 0 {
			return fmt.Errorf("store: shard count must be a positive power of two")
		}
		c.shards = n
		return nil
	})
}

// WithMaxCost bounds the sum of entry costs. A zero limit means unbounded.
// Entries with a cost larger than maxCost are rejected. cost's key and value
// types must match the store's.
func WithMaxCost[K comparable, V any](maxCost int64, cost func(K, V) int64) Option {
	return storeOption(func(c *storeConfig) error {
		if maxCost < 0 {
			return fmt.Errorf("store: max cost cannot be negative")
		}
		if maxCost > 0 && cost == nil {
			return fmt.Errorf("store: a cost function is required with max cost")
		}
		c.maxCost, c.cost = maxCost, cost
		return nil
	})
}

// WithCleanupInterval enables periodic removal of expired entries. Expired
// entries are always hidden and lazily removed even when cleanup is disabled.
func WithCleanupInterval(interval time.Duration) Option {
	return storeOption(func(c *storeConfig) error {
		c.cleanupInterval = interval
		return nonNegative("cleanup interval", interval)
	})
}

// WithEvictionCallback registers a synchronous callback for expiry and
// capacity evictions. It is not called for Delete, Clear, replacement, or
// cached absences. fn's key and value types must match the store's.
func WithEvictionCallback[K comparable, V any](fn func(K, V, EvictionReason)) Option {
	return storeOption(func(c *storeConfig) error {
		c.onEvict = fn
		return nil
	})
}

// WithTTLJitter lengthens each positive TTL by a random amount of up to
// fraction*TTL, so entries written together do not expire together.
func WithTTLJitter(fraction float64) Option {
	return storeOption(func(c *storeConfig) error {
		if fraction < 0 || fraction > 1 {
			return fmt.Errorf("store: TTL jitter must be between 0 and 1")
		}
		c.jitter = fraction
		return nil
	})
}

// WithNegativeTTL makes GetOrLoad store an ok=false Loader result for ttl, so
// repeated lookups of a missing key do not reach the Loader. Errors are never
// cached. Get, SetNX, and CompareAndSwap treat a cached absence as no value.
func WithNegativeTTL(ttl time.Duration) Option {
	return storeOption(func(c *storeConfig) error {
		c.negativeTTL = ttl
		return nonNegative("negative TTL", ttl)
	})
}

// WithStaleOnError keeps expired entries for grace after they expire. Get
// never returns them, but when a GetOrLoad reload fails, callers receive the
// expired value with a nil error instead of the load error. The failure is
// visible only through Stats: LoadErrors and StaleFallbacks.
func WithStaleOnError(grace time.Duration) Option {
	return storeOption(func(c *storeConfig) error {
		c.staleGrace = grace
		return nonNegative("stale grace", grace)
	})
}

// WithRefreshAhead makes GetOrLoad reload a hit in the background once the
// entry has less than window left to live. The caller receives the current
// value immediately; a failed refresh is retried on a later hit.
func WithRefreshAhead(window time.Duration) Option {
	return storeOption(func(c *storeConfig) error {
		c.refreshAhead = window
		return nonNegative("refresh-ahead window", window)
	})
}
