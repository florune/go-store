package store

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// Loader obtains a missing value. ok=false reports that no value exists; such
// results are returned to callers but not stored.
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
	value   V
	ok      bool
	err     error
}

// GetOrLoad returns a cached value or coalesces concurrent misses for the same
// key into one Loader call.
//
// The Loader runs on its own goroutine with a context that keeps the first
// caller's values but not its cancellation. That context is canceled only
// after every waiting caller has left because its own context ended. Each
// caller returns when the load finishes or its own context ends.
//
// A successful result is stored with ttl unless it exceeds MaxCost or key was
// written by Set, SetNX, CompareAndSwap, Add, or Delete while the load was in
// flight; callers receive the loaded value either way. A Loader panic is
// returned as *LoaderPanicError.
func (s *Store[K, V]) GetOrLoad(ctx context.Context, key K, ttl time.Duration, loader Loader[K, V]) (V, bool, error) {
	if value, ok := s.lookup(key); ok {
		s.hits.Add(1)
		return value, true, nil
	}
	if err := ctx.Err(); err != nil {
		s.misses.Add(1)
		var zero V
		return zero, false, err
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	if e := sh.items[key]; e != nil {
		if now := time.Now().UnixNano(); !isExpired(e, now) {
			value := e.value
			e.accessed.Store(now)
			sh.mu.Unlock()
			s.hits.Add(1)
			return value, true, nil
		}
	}
	call := sh.flights[key]
	if call == nil {
		loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		call = &flight[V]{done: make(chan struct{}), cancel: cancel}
		sh.flights[key] = call
		s.loads.Add(1)
		go s.load(loadCtx, sh, key, ttl, loader, call)
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
	}

	var e, old *entry[V]
	if call.err == nil && call.ok {
		if cost, fits := s.costOf(key, call.value); fits {
			e = newEntry(call.value, ttl, time.Now().UnixNano(), cost)
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
	}
	sh.mu.Unlock()
	close(call.done)

	if !store {
		return
	}
	delta := e.cost
	if old != nil {
		// Any write that left a live entry would have marked the flight
		// stale, so old is an expired entry this load replaces.
		delta -= old.cost
		s.evicted(key, old, Expired)
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
