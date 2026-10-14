// Package store provides typed, sharded, in-process stores with expiration,
// bounded capacity, and request-coalesced loading.
package store

import (
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// epoch anchors the store clock. time.Since reads the monotonic clock, so TTLs
// are unaffected by wall-clock adjustments.
var epoch = time.Now()

func monotonicNow() int64 { return int64(time.Since(epoch)) }

// entry is immutable once published, apart from its atomic fields. Changing a
// key's value or expiry replaces its entry, so readers may use an entry after
// releasing the shard lock.
type entry[V any] struct {
	value     V
	expiresAt int64 // zero means the entry never expires
	cost      int64
	// absent marks a cached "no value" result from a Loader.
	absent     bool
	accessed   atomic.Int64
	refreshing atomic.Bool
}

func isLive[V any](e *entry[V], now int64) bool {
	return e.expiresAt == 0 || now < e.expiresAt
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
	// NegativeHits counts lookups answered by a cached absence (WithNegativeTTL).
	NegativeHits uint64
	// StaleFallbacks counts failed loads that returned the previous value
	// instead of the error (WithStaleOnError).
	StaleFallbacks uint64
	// Refreshes counts background loads started by WithRefreshAhead.
	Refreshes uint64
	Entries   int
	Cost      int64
}

// Store is a concurrency-safe, typed in-memory key/value store. Values use Go
// assignment semantics: callers must not mutate shared maps, pointers, slices,
// or reference-bearing fields concurrently. Use Bytes for owned byte slices.
type Store[K comparable, V any] struct {
	seed         maphash.Seed
	shards       []shard[K, V]
	mask         uint64
	name         string
	maxCost      int64
	costFn       func(K, V) int64
	onEvict      func(K, V, EvictionReason)
	jitter       float64
	negativeTTL  time.Duration
	loadTimeout  time.Duration
	staleGrace   int64
	refreshAhead int64
	clock        func() int64

	cost           atomic.Int64
	hits           atomic.Uint64
	misses         atomic.Uint64
	evictions      atomic.Uint64
	loads          atomic.Uint64
	loadErrs       atomic.Uint64
	negativeHits   atomic.Uint64
	staleFallbacks atomic.Uint64
	refreshes      atomic.Uint64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// New constructs a store. Call Close when it is no longer needed: it stops
// periodic cleanup and releases the store's name.
func New[K comparable, V any](options ...Option) (*Store[K, V], error) {
	c := storeConfig{shards: defaultShards}
	for _, option := range options {
		if err := option.applyStore(&c); err != nil {
			return nil, err
		}
	}
	var costFn func(K, V) int64
	if c.cost != nil {
		fn, ok := c.cost.(func(K, V) int64)
		if !ok {
			return nil, fmt.Errorf("store: WithMaxCost cost function is %T, want %T", c.cost, fn)
		}
		costFn = fn
	}
	var onEvict func(K, V, EvictionReason)
	if c.onEvict != nil {
		fn, ok := c.onEvict.(func(K, V, EvictionReason))
		if !ok {
			return nil, fmt.Errorf("store: WithEvictionCallback callback is %T, want %T", c.onEvict, fn)
		}
		onEvict = fn
	}
	s := &Store[K, V]{
		seed: maphash.MakeSeed(), shards: make([]shard[K, V], c.shards),
		mask: uint64(c.shards - 1), name: c.name, maxCost: c.maxCost, costFn: costFn,
		onEvict: onEvict, jitter: c.jitter, negativeTTL: c.negativeTTL,
		loadTimeout: c.loadTimeout, staleGrace: int64(c.staleGrace),
		refreshAhead: int64(c.refreshAhead), clock: monotonicNow,
	}
	for i := range s.shards {
		s.shards[i].items = make(map[K]*entry[V])
		s.shards[i].flights = make(map[K]*flight[V])
	}
	if s.name != "" {
		if err := register(s.name, func() any { return s.Stats() }); err != nil {
			return nil, err
		}
	}
	if c.cleanupInterval > 0 {
		s.stop, s.done = make(chan struct{}), make(chan struct{})
		go s.cleanupLoop(c.cleanupInterval)
	}
	return s, nil
}

// MustNew is New with option errors promoted to panics.
func MustNew[K comparable, V any](options ...Option) *Store[K, V] {
	s, err := New[K, V](options...)
	if err != nil {
		panic(err)
	}
	return s
}

// Name returns the name given by WithName, or "".
func (s *Store[K, V]) Name() string { return s.name }

func (s *Store[K, V]) shardFor(key K) *shard[K, V] {
	return &s.shards[maphash.Comparable(s.seed, key)&s.mask]
}

// expiry returns the expiry for ttl, spread by the configured jitter.
func (s *Store[K, V]) expiry(ttl time.Duration, now int64) int64 {
	if ttl <= 0 {
		return 0
	}
	if s.jitter > 0 {
		ttl += time.Duration(rand.Int64N(int64(float64(ttl)*s.jitter) + 1))
	}
	return now + int64(ttl)
}

// reclaimable reports whether an entry is expired and past the stale grace
// period, so it can no longer serve as a fallback.
func (s *Store[K, V]) reclaimable(e *entry[V], now int64) bool {
	return e.expiresAt > 0 && now >= e.expiresAt+s.staleGrace
}

func (s *Store[K, V]) newEntry(value V, ttl time.Duration, now, cost int64) *entry[V] {
	e := &entry[V]{value: value, expiresAt: s.expiry(ttl, now), cost: cost}
	e.accessed.Store(now)
	return e
}

func (s *Store[K, V]) newAbsentEntry(now int64) *entry[V] {
	e := &entry[V]{expiresAt: s.expiry(s.negativeTTL, now), cost: 1, absent: true}
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
	e := s.newEntry(value, ttl, s.clock(), cost)
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

// SetNX stores value only when key has no live value. A cached absence does
// not count as a value.
func (s *Store[K, V]) SetNX(key K, value V, ttl time.Duration) bool {
	cost, fits := s.costOf(key, value)
	if !fits {
		return false
	}
	now := s.clock()
	sh := s.shardFor(key)
	sh.mu.Lock()
	old := sh.items[key]
	if old != nil && isLive(old, now) && !old.absent {
		sh.mu.Unlock()
		return false
	}
	sh.items[key] = s.newEntry(value, ttl, now, cost)
	sh.invalidateFlight(key)
	sh.mu.Unlock()
	delta := cost
	if old != nil {
		delta -= old.cost
		if !isLive(old, now) {
			s.evicted(key, old, Expired)
		}
	}
	s.cost.Add(delta)
	s.enforceCapacity()
	return true
}

// Get returns a live value and updates its recency. A cached absence is
// reported as a miss.
func (s *Store[K, V]) Get(key K) (V, bool) {
	if e := s.peek(key, s.clock()); e != nil && !e.absent {
		s.hits.Add(1)
		return e.value, true
	}
	s.misses.Add(1)
	var zero V
	return zero, false
}

// peek returns key's live entry and records the access. An expired entry is
// removed once it is past the stale grace period.
func (s *Store[K, V]) peek(key K, now int64) *entry[V] {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e := sh.items[key]
	sh.mu.RUnlock()
	if e == nil {
		return nil
	}
	if isLive(e, now) {
		e.accessed.Store(now)
		return e
	}
	if s.reclaimable(e, now) {
		s.removeIfSame(key, e, Expired)
	}
	return nil
}

// Delete removes key and reports whether an entry existed. Any load of key
// already in flight will not store its result.
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

// Clear removes every entry and keeps loads already in flight from storing
// their results. Eviction callbacks are not called.
func (s *Store[K, V]) Clear() {
	for i := range s.shards {
		sh := &s.shards[i]
		var freed int64
		sh.mu.Lock()
		for _, e := range sh.items {
			freed += e.cost
		}
		clear(sh.items)
		for _, call := range sh.flights {
			call.stale = true
		}
		sh.mu.Unlock()
		s.cost.Add(-freed)
	}
}

// Expire changes a live key's TTL. A non-positive TTL removes expiration. It
// returns false for a missing key or a cached absence.
func (s *Store[K, V]) Expire(key K, ttl time.Duration) bool {
	now := s.clock()
	sh := s.shardFor(key)
	sh.mu.Lock()
	e := sh.items[key]
	if e == nil || e.absent {
		sh.mu.Unlock()
		return false
	}
	if !isLive(e, now) {
		delete(sh.items, key)
		sh.mu.Unlock()
		s.cost.Add(-e.cost)
		s.evicted(key, e, Expired)
		return false
	}
	sh.items[key] = s.newEntry(e.value, ttl, now, e.cost)
	sh.mu.Unlock()
	return true
}

// Len returns the current number of entries, including cached absences and
// expired entries not yet reclaimed.
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
	return Stats{
		Hits: s.hits.Load(), Misses: s.misses.Load(), Evictions: s.evictions.Load(),
		Loads: s.loads.Load(), LoadErrors: s.loadErrs.Load(),
		NegativeHits: s.negativeHits.Load(), StaleFallbacks: s.staleFallbacks.Load(),
		Refreshes: s.refreshes.Load(), Entries: s.Len(), Cost: s.cost.Load(),
	}
}

// Close stops background cleanup and releases the store's name. It is safe to
// call more than once.
func (s *Store[K, V]) Close() {
	s.closeOnce.Do(func() {
		if s.stop != nil {
			close(s.stop)
			<-s.done
		}
		if s.name != "" {
			unregister(s.name)
		}
	})
}

func (s *Store[K, V]) cleanupLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer func() { t.Stop(); close(s.done) }()
	for {
		select {
		case <-t.C:
			s.removeExpired(s.clock())
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
			if s.reclaimable(e, now) {
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

// evicted records an eviction. Cached absences carry no value, so the
// eviction callback is not called for them.
func (s *Store[K, V]) evicted(key K, e *entry[V], reason EvictionReason) {
	s.evictions.Add(1)
	if s.onEvict != nil && !e.absent {
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
