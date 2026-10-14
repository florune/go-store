// Package store provides typed, sharded, in-process stores with expiration,
// bounded capacity, and request-coalesced loading.
package store

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

type entry[V any] struct {
	value     V
	expiresAt int64
	cost      int64
	accessed  atomic.Int64
}

type shard[K comparable, V any] struct {
	mu      sync.RWMutex
	items   map[K]*entry[V]
	flights map[K]*flight[V]
}

// invalidateFlight keeps an in-flight load of key from storing its result,
// which may predate the write being made. Callers hold sh.mu.
func (sh *shard[K, V]) invalidateFlight(key K) {
	if call := sh.flights[key]; call != nil {
		call.stale = true
	}
}

// Stats is a point-in-time snapshot of store activity.
type Stats struct {
	Hits, Misses, Evictions, Loads, LoadErrors uint64
	Entries                                    int
	Cost                                       int64
}

// Store is a concurrency-safe, typed in-memory key/value store. Values use Go
// assignment semantics: callers must not mutate shared maps, pointers, slices,
// or reference-bearing fields concurrently. Use Bytes for owned byte slices.
type Store[K comparable, V any] struct {
	seed    maphash.Seed
	shards  []shard[K, V]
	mask    uint64
	maxCost int64
	costFn  func(K, V) int64
	onEvict func(K, V, EvictionReason)

	cost      atomic.Int64
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	loads     atomic.Uint64
	loadErrs  atomic.Uint64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// New constructs a store. Call Close when periodic cleanup is enabled.
func New[K comparable, V any](options ...Option[K, V]) (*Store[K, V], error) {
	c := config[K, V]{shards: defaultShards}
	for _, option := range options {
		if err := option(&c); err != nil {
			return nil, err
		}
	}
	s := &Store[K, V]{
		seed: maphash.MakeSeed(), shards: make([]shard[K, V], c.shards),
		mask: uint64(c.shards - 1), maxCost: c.maxCost, costFn: c.cost,
		onEvict: c.onEvict,
	}
	for i := range s.shards {
		s.shards[i].items = make(map[K]*entry[V])
		s.shards[i].flights = make(map[K]*flight[V])
	}
	if c.cleanupInterval > 0 {
		s.stop, s.done = make(chan struct{}), make(chan struct{})
		go s.cleanupLoop(c.cleanupInterval)
	}
	return s, nil
}

// MustNew is New with option errors promoted to panics.
func MustNew[K comparable, V any](options ...Option[K, V]) *Store[K, V] {
	s, err := New(options...)
	if err != nil {
		panic(err)
	}
	return s
}

func (s *Store[K, V]) shardFor(key K) *shard[K, V] {
	return &s.shards[maphash.Comparable(s.seed, key)&s.mask]
}

func expiry(ttl time.Duration, now int64) int64 {
	if ttl <= 0 {
		return 0
	}
	return now + int64(ttl)
}

func isExpired[V any](e *entry[V], now int64) bool {
	return e.expiresAt > 0 && now >= e.expiresAt
}

func newEntry[V any](value V, ttl time.Duration, now, cost int64) *entry[V] {
	e := &entry[V]{value: value, expiresAt: expiry(ttl, now), cost: cost}
	e.accessed.Store(now)
	return e
}

// costOf returns value's cost and whether it fits within MaxCost.
func (s *Store[K, V]) costOf(key K, value V) (int64, bool) {
	cost := int64(1)
	if s.costFn != nil {
		cost = max(s.costFn(key, value), 0)
	}
	return cost, s.maxCost <= 0 || cost <= s.maxCost
}

// Set stores value. A zero or negative TTL means no expiration. It returns
// false without changing an existing value if the item alone exceeds MaxCost.
func (s *Store[K, V]) Set(key K, value V, ttl time.Duration) bool {
	cost, fits := s.costOf(key, value)
	if !fits {
		return false
	}
	e := newEntry(value, ttl, time.Now().UnixNano(), cost)
	sh := s.shardFor(key)
	sh.mu.Lock()
	old := sh.items[key]
	sh.items[key] = e
	sh.invalidateFlight(key)
	sh.mu.Unlock()
	delta := cost
	if old != nil {
		delta -= old.cost
	}
	s.cost.Add(delta)
	s.enforceCapacity()
	return true
}

// SetNX stores value only when key has no live value.
func (s *Store[K, V]) SetNX(key K, value V, ttl time.Duration) bool {
	cost, fits := s.costOf(key, value)
	if !fits {
		return false
	}
	now := time.Now().UnixNano()
	sh := s.shardFor(key)
	var expired *entry[V]
	sh.mu.Lock()
	if current := sh.items[key]; current != nil {
		if !isExpired(current, now) {
			sh.mu.Unlock()
			return false
		}
		expired = current
	}
	sh.items[key] = newEntry(value, ttl, now, cost)
	sh.invalidateFlight(key)
	sh.mu.Unlock()
	delta := cost
	if expired != nil {
		delta -= expired.cost
		s.evicted(key, expired, Expired)
	}
	s.cost.Add(delta)
	s.enforceCapacity()
	return true
}

// Get returns a live value and updates its recency.
func (s *Store[K, V]) Get(key K) (V, bool) {
	value, ok := s.lookup(key)
	if ok {
		s.hits.Add(1)
	} else {
		s.misses.Add(1)
	}
	return value, ok
}

// lookup is Get without hit and miss accounting.
func (s *Store[K, V]) lookup(key K) (V, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e := sh.items[key]
	if e == nil {
		sh.mu.RUnlock()
		var zero V
		return zero, false
	}
	now := time.Now().UnixNano()
	if isExpired(e, now) {
		sh.mu.RUnlock()
		s.removeIfSame(key, e, Expired)
		var zero V
		return zero, false
	}
	value := e.value
	e.accessed.Store(now)
	sh.mu.RUnlock()
	return value, true
}

// Delete removes key and reports whether a live or expired entry existed.
func (s *Store[K, V]) Delete(key K) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	e := sh.items[key]
	if e != nil {
		delete(sh.items, key)
	}
	sh.invalidateFlight(key)
	sh.mu.Unlock()
	if e != nil {
		s.cost.Add(-e.cost)
	}
	return e != nil
}

// Expire changes a live key's TTL. A non-positive TTL removes expiration.
func (s *Store[K, V]) Expire(key K, ttl time.Duration) bool {
	now := time.Now().UnixNano()
	sh := s.shardFor(key)
	sh.mu.Lock()
	e := sh.items[key]
	if e == nil || isExpired(e, now) {
		if e != nil {
			delete(sh.items, key)
		}
		sh.mu.Unlock()
		if e != nil {
			s.cost.Add(-e.cost)
			s.evicted(key, e, Expired)
		}
		return false
	}
	e.expiresAt = expiry(ttl, now)
	sh.mu.Unlock()
	return true
}

// updateExisting applies fn and refreshes TTL while the key remains pinned in
// its shard. It is an internal seam used by compound structures.
func (s *Store[K, V]) updateExisting(key K, ttl time.Duration, fn func(V)) bool {
	now := time.Now().UnixNano()
	sh := s.shardFor(key)
	sh.mu.Lock()
	e := sh.items[key]
	if e == nil {
		sh.mu.Unlock()
		return false
	}
	if isExpired(e, now) {
		delete(sh.items, key)
		sh.mu.Unlock()
		s.cost.Add(-e.cost)
		s.evicted(key, e, Expired)
		return false
	}
	fn(e.value)
	e.expiresAt = expiry(ttl, now)
	e.accessed.Store(now)
	sh.mu.Unlock()
	return true
}

// Len returns the current number of entries, including entries that expired
// since the last lazy or periodic cleanup.
func (s *Store[K, V]) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.items)
		sh.mu.RUnlock()
	}
	return n
}

func (s *Store[K, V]) Stats() Stats {
	return Stats{Hits: s.hits.Load(), Misses: s.misses.Load(), Evictions: s.evictions.Load(), Loads: s.loads.Load(), LoadErrors: s.loadErrs.Load(), Entries: s.Len(), Cost: s.cost.Load()}
}

// Close stops background cleanup. It is safe to call more than once.
func (s *Store[K, V]) Close() {
	s.closeOnce.Do(func() {
		if s.stop != nil {
			close(s.stop)
			<-s.done
		}
	})
}

func (s *Store[K, V]) cleanupLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer func() { t.Stop(); close(s.done) }()
	for {
		select {
		case now := <-t.C:
			s.removeExpired(now.UnixNano())
		case <-s.stop:
			return
		}
	}
}

func (s *Store[K, V]) removeExpired(now int64) {
	for i := range s.shards {
		sh := &s.shards[i]
		var removed []struct {
			key K
			e   *entry[V]
		}
		sh.mu.Lock()
		for key, e := range sh.items {
			if isExpired(e, now) {
				delete(sh.items, key)
				removed = append(removed, struct {
					key K
					e   *entry[V]
				}{key, e})
			}
		}
		sh.mu.Unlock()
		for _, item := range removed {
			s.cost.Add(-item.e.cost)
			s.evicted(item.key, item.e, Expired)
		}
	}
}

func (s *Store[K, V]) removeIfSame(key K, target *entry[V], reason EvictionReason) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	if sh.items[key] != target {
		sh.mu.Unlock()
		return false
	}
	delete(sh.items, key)
	sh.mu.Unlock()
	s.cost.Add(-target.cost)
	s.evicted(key, target, reason)
	return true
}

func (s *Store[K, V]) evicted(key K, e *entry[V], reason EvictionReason) {
	s.evictions.Add(1)
	if s.onEvict != nil {
		s.onEvict(key, e.value, reason)
	}
}

func (s *Store[K, V]) enforceCapacity() {
	for s.maxCost > 0 && s.cost.Load() > s.maxCost {
		var candidateKey K
		var candidate *entry[V]
		found := false
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.RLock()
			sampled := 0
			for key, e := range sh.items {
				if !found || e.accessed.Load() < candidate.accessed.Load() {
					candidateKey, candidate, found = key, e, true
				}
				sampled++
				if sampled == 5 {
					break
				}
			}
			sh.mu.RUnlock()
		}
		if !found || !s.removeIfSame(candidateKey, candidate, Capacity) {
			return
		}
	}
}
