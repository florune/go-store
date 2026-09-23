package store

import (
	"context"
	"time"
)

// Loader obtains a missing value. ok=false is a cacheable decision only for
// the current call; negative results are not stored.
type Loader[K comparable, V any] func(context.Context, K) (value V, ok bool, err error)

type flight[V any] struct {
	done  chan struct{}
	value V
	ok    bool
	err   error
}

// GetOrLoad returns a cached value or coalesces concurrent loads for the same
// key. A waiting caller may leave when its own context is canceled.
func (s *Store[K, V]) GetOrLoad(ctx context.Context, key K, ttl time.Duration, loader Loader[K, V]) (V, bool, error) {
	if value, ok := s.Get(key); ok {
		return value, true, nil
	}
	s.flightsMu.Lock()
	if call := s.flights[key]; call != nil {
		s.flightsMu.Unlock()
		select {
		case <-call.done:
			return call.value, call.ok, call.err
		case <-ctx.Done():
			var zero V
			return zero, false, ctx.Err()
		}
	}
	call := &flight[V]{done: make(chan struct{})}
	s.flights[key] = call
	s.flightsMu.Unlock()

	if value, ok := s.Get(key); ok {
		call.value, call.ok = value, true
	} else {
		s.loads.Add(1)
		call.value, call.ok, call.err = loader(ctx, key)
		if call.err != nil {
			s.loadErrs.Add(1)
		}
		if call.err == nil && call.ok {
			call.ok = s.Set(key, call.value, ttl)
		}
	}
	s.flightsMu.Lock()
	delete(s.flights, key)
	close(call.done)
	s.flightsMu.Unlock()
	return call.value, call.ok, call.err
}
