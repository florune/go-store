package store

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// Loader obtains a missing value. ok=false reports that no value exists; such
// results are returned to callers and stored only when WithNegativeTTL is set.
type Loader[K comparable, V any] func(context.Context, K) (value V, ok bool, err error)

// LoaderPanicError is returned to every caller sharing a load whose Loader
// panicked.
type LoaderPanicError struct {
	Value any
	Stack []byte
}

func (e *LoaderPanicError) Error() string {
	return fmt.Sprintf("store: loader panicked: %v", e.Value)
}

// Unwrap returns the panic value when it is an error.
func (e *LoaderPanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// flight is one shared Loader call. waiters and stale are guarded by the
// owning shard's lock; value, ok, and err are published by closing done.
type flight[V any] struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	stale   bool
	// refreshOf is the live entry a WithRefreshAhead load is replacing.
	refreshOf *entry[V]
	value     V
	ok        bool
	err       error
}

// GetOrLoad returns a cached value or coalesces concurrent misses for the same
// key into one Loader call.
//
// The Loader runs on its own goroutine with a context that keeps the first
// caller's values but not its cancellation, bounded by WithLoadTimeout. That
// context is canceled after every waiting caller has left because its own
// context ended. Each caller returns when the load finishes or its own
// context ends.
//
// A successful result is stored with ttl unless it exceeds MaxCost or key was
// written by Set, SetNX, CompareAndSwap, Add, Delete, or Clear while the load
// was in flight; callers receive the loaded value either way. With
// WithNegativeTTL, an ok=false result is stored as a cached absence and later
// calls return ok=false without loading. With WithStaleOnError, a failed load
// returns the previous value, if one is still within the grace period, with a
// nil error: callers cannot tell it apart from a fresh value, and the failure
// shows only in Stats (LoadErrors, StaleFallbacks). A Loader panic is returned
// as *LoaderPanicError.
func (s *Store[K, V]) GetOrLoad(ctx context.Context, key K, ttl time.Duration, loader Loader[K, V]) (V, bool, error) {
	now := s.clock()
	if e := s.peek(key, now); e != nil {
		s.maybeRefresh(ctx, key, e, now, ttl, loader)
		return s.hit(e)
	}
	if err := ctx.Err(); err != nil {
		s.misses.Add(1)
		var zero V
		return zero, false, err
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	if e := sh.items[key]; e != nil {
		if now := s.clock(); isLive(e, now) {
			e.accessed.Store(now)
			sh.mu.Unlock()
			return s.hit(e)
		}
	}
	call := sh.flights[key]
	if call == nil {
		call = s.startLoad(ctx, sh, key, ttl, loader, nil)
	}
	call.waiters++
	sh.mu.Unlock()
	s.misses.Add(1)

	select {
	case <-call.done:
		return call.value, call.ok, call.err
	case <-ctx.Done():
		s.abandon(sh, key, call)
		var zero V
		return zero, false, ctx.Err()
	}
}

func (s *Store[K, V]) hit(e *entry[V]) (V, bool, error) {
	if e.absent {
		s.negativeHits.Add(1)
		var zero V
		return zero, false, nil
	}
	s.hits.Add(1)
	return e.value, true, nil
}

// maybeRefresh starts a background load of key when its live entry is within
// the WithRefreshAhead window. At most one refresh runs per entry.
func (s *Store[K, V]) maybeRefresh(ctx context.Context, key K, e *entry[V], now int64, ttl time.Duration, loader Loader[K, V]) {
	if s.refreshAhead <= 0 || e.absent || e.expiresAt == 0 || e.expiresAt-now > s.refreshAhead {
		return
	}
	if !e.refreshing.CompareAndSwap(false, true) {
		return
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	if sh.items[key] == e && sh.flights[key] == nil {
		s.refreshes.Add(1)
		s.startLoad(ctx, sh, key, ttl, loader, e)
	}
	sh.mu.Unlock()
}

// startLoad registers and starts a load of key. refreshOf is the live entry a
// refresh replaces, or nil. Callers hold sh.mu.
func (s *Store[K, V]) startLoad(ctx context.Context, sh *shard[K, V], key K, ttl time.Duration, loader Loader[K, V], refreshOf *entry[V]) *flight[V] {
	loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if s.loadTimeout > 0 {
		loadCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), s.loadTimeout)
	}
	call := &flight[V]{done: make(chan struct{}), cancel: cancel, refreshOf: refreshOf}
	sh.flights[key] = call
	s.loads.Add(1)
	go s.load(loadCtx, sh, key, ttl, loader, call)
	return call
}

func (s *Store[K, V]) load(ctx context.Context, sh *shard[K, V], key K, ttl time.Duration, loader Loader[K, V], call *flight[V]) {
	defer call.cancel()
	func() {
		defer func() {
			if r := recover(); r != nil {
				call.err = &LoaderPanicError{Value: r, Stack: debug.Stack()}
			}
		}()
		call.value, call.ok, call.err = loader(ctx, key)
	}()
	if call.err != nil {
		s.loadErrs.Add(1)
		if call.refreshOf != nil {
			call.refreshOf.refreshing.Store(false)
		}
	}

	now := s.clock()
	var e, old *entry[V]
	if call.err == nil {
		if call.ok {
			if cost, fits := s.costOf(key, call.value); fits {
				e = s.newEntry(call.value, ttl, now, cost)
			}
		} else if s.negativeTTL > 0 {
			e = s.newAbsentEntry(now)
		}
	}
	sh.mu.Lock()
	if sh.flights[key] == call {
		delete(sh.flights, key)
	}
	store := e != nil && !call.stale
	if store {
		old = sh.items[key]
		sh.items[key] = e
	} else if call.err != nil && s.staleGrace > 0 {
		if cur := sh.items[key]; cur != nil && !s.reclaimable(cur, now) {
			var zero V
			call.value, call.ok, call.err = zero, false, nil
			if !cur.absent {
				call.value, call.ok = cur.value, true
			}
			s.staleFallbacks.Add(1)
		}
	}
	sh.mu.Unlock()
	close(call.done)

	if !store {
		return
	}
	delta := e.cost
	if old != nil {
		delta -= old.cost
		// Writes made during the load mark it stale, so old is either the
		// live entry this refresh replaces or an expired entry.
		if !isLive(old, now) {
			s.evicted(key, old, Expired)
		}
	}
	s.cost.Add(delta)
	s.enforceCapacity()
}

// abandon drops a caller whose context ended. The last caller to leave an
// unfinished load cancels it and unregisters it, so later callers start a
// fresh load and the abandoned result is never stored.
func (s *Store[K, V]) abandon(sh *shard[K, V], key K, call *flight[V]) {
	sh.mu.Lock()
	call.waiters--
	last := call.waiters == 0 && sh.flights[key] == call
	if last {
		delete(sh.flights, key)
		call.stale = true
	}
	sh.mu.Unlock()
	if last {
		call.cancel()
	}
}
