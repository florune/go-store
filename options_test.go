package store

import (
	"context"
	"errors"
	"expvar"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock drives a store's TTL clock in tests.
type fakeClock struct{ now atomic.Int64 }

func (c *fakeClock) read() int64             { return c.now.Load() }
func (c *fakeClock) advance(d time.Duration) { c.now.Add(int64(d)) }

func newClockedStore[K comparable, V any](t *testing.T, options ...Option) (*Store[K, V], *fakeClock) {
	t.Helper()
	s := MustNew[K, V](options...)
	t.Cleanup(s.Close)
	c := &fakeClock{}
	c.now.Store(int64(time.Hour))
	s.clock = c.read
	return s, c
}

// countingLoader returns value, ok, and err and counts its calls.
func countingLoader(calls *atomic.Int32, value int, ok bool, err error) Loader[string, int] {
	return func(context.Context, string) (int, bool, error) {
		calls.Add(1)
		return value, ok, err
	}
}

func TestTTLJitterSpreadsExpiry(t *testing.T) {
	s, c := newClockedStore[int, int](t, WithTTLJitter(0.5))
	const ttl = 100 * time.Second
	start := c.read()
	seen := make(map[int64]bool)
	for i := range 200 {
		s.Set(i, i, ttl)
		e := s.shardFor(i).items[i]
		if e.expiresAt < start+int64(ttl) || e.expiresAt > start+int64(ttl)*3/2 {
			t.Fatalf("expiry %v outside [ttl, 1.5*ttl]", time.Duration(e.expiresAt-start))
		}
		seen[e.expiresAt] = true
	}
	if len(seen) < 100 {
		t.Fatalf("only %d distinct expiries out of 200", len(seen))
	}
	if err := WithTTLJitter(1.5).applyStore(&storeConfig{}); err == nil {
		t.Fatal("jitter above 1 accepted")
	}
}

func TestNegativeTTLCachesAbsence(t *testing.T) {
	s, c := newClockedStore[string, int](t, WithNegativeTTL(10*time.Second))
	var calls atomic.Int32
	missing := countingLoader(&calls, 0, false, nil)

	for range 3 {
		if v, ok, err := s.GetOrLoad(context.Background(), "k", time.Minute, missing); v != 0 || ok || err != nil {
			t.Fatalf("GetOrLoad = %v, %v, %v", v, ok, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader called %d times within negative TTL", calls.Load())
	}
	if st := s.Stats(); st.NegativeHits != 2 || st.Misses != 1 || st.Hits != 0 {
		t.Fatalf("stats = %+v", st)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("Get returned a cached absence as a value")
	}
	if s.Expire("k", time.Hour) {
		t.Fatal("Expire extended a cached absence")
	}

	c.advance(11 * time.Second)
	s.GetOrLoad(context.Background(), "k", time.Minute, missing)
	if calls.Load() != 2 {
		t.Fatalf("loader not called again after negative TTL; calls = %d", calls.Load())
	}

	if !s.SetNX("k", 7, 0) {
		t.Fatal("SetNX refused to replace a cached absence")
	}
	if got, ok := s.Get("k"); !ok || got != 7 {
		t.Fatalf("Get after SetNX = %v, %v", got, ok)
	}
}

func TestNegativeTTLIsNotUsedForErrors(t *testing.T) {
	s, _ := newClockedStore[string, int](t, WithNegativeTTL(time.Minute))
	var calls atomic.Int32
	failing := countingLoader(&calls, 0, false, errors.New("db down"))
	s.GetOrLoad(context.Background(), "k", time.Minute, failing)
	s.GetOrLoad(context.Background(), "k", time.Minute, failing)
	if calls.Load() != 2 {
		t.Fatalf("an error result was cached; calls = %d", calls.Load())
	}
}

func TestDeleteRemovesCachedAbsence(t *testing.T) {
	s, _ := newClockedStore[string, int](t, WithNegativeTTL(time.Minute))
	var calls atomic.Int32
	s.GetOrLoad(context.Background(), "k", time.Minute, countingLoader(&calls, 0, false, nil))
	if !s.Delete("k") {
		t.Fatal("Delete did not find the cached absence")
	}
	if got, ok, _ := s.GetOrLoad(context.Background(), "k", time.Minute, countingLoader(&calls, 5, true, nil)); !ok || got != 5 {
		t.Fatalf("GetOrLoad after Delete = %v, %v", got, ok)
	}
}

func TestEvictionCallbackSkipsCachedAbsence(t *testing.T) {
	var evicted atomic.Int32
	s, c := newClockedStore[string, int](t,
		WithNegativeTTL(time.Second),
		WithEvictionCallback(func(string, int, EvictionReason) { evicted.Add(1) }),
	)
	var calls atomic.Int32
	s.GetOrLoad(context.Background(), "k", 0, countingLoader(&calls, 0, false, nil))
	c.advance(2 * time.Second)
	s.removeExpired(c.read())
	if evicted.Load() != 0 || s.Len() != 0 {
		t.Fatalf("callbacks = %d, Len = %d", evicted.Load(), s.Len())
	}
}

func TestLoadTimeout(t *testing.T) {
	s := MustNew[string, int](WithLoadTimeout(20 * time.Millisecond))
	_, ok, err := s.GetOrLoad(context.Background(), "k", 0, func(ctx context.Context, _ string) (int, bool, error) {
		<-ctx.Done()
		return 0, false, ctx.Err()
	})
	if ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GetOrLoad = %v, %v; want DeadlineExceeded", ok, err)
	}
}

func TestStaleOnErrorServesExpiredValue(t *testing.T) {
	s, c := newClockedStore[string, int](t, WithStaleOnError(time.Minute))
	var calls atomic.Int32
	failing := countingLoader(&calls, 0, false, errors.New("redis down"))
	s.Set("k", 1, 10*time.Second)
	c.advance(11 * time.Second)

	if _, ok := s.Get("k"); ok {
		t.Fatal("Get returned an expired value")
	}
	got, ok, err := s.GetOrLoad(context.Background(), "k", 10*time.Second, failing)
	if err != nil || !ok || got != 1 {
		t.Fatalf("GetOrLoad = %v, %v, %v; want stale value 1", got, ok, err)
	}
	if st := s.Stats(); st.StaleFallbacks != 1 || st.LoadErrors != 1 {
		t.Fatalf("stats = %+v", st)
	}

	c.advance(time.Minute)
	if _, _, err := s.GetOrLoad(context.Background(), "k", 10*time.Second, failing); err == nil {
		t.Fatal("value past the grace period was served")
	}
}

func TestStaleOnErrorKeepsEntriesThroughCleanup(t *testing.T) {
	s, c := newClockedStore[string, int](t, WithStaleOnError(time.Minute))
	s.Set("k", 1, time.Second)
	c.advance(2 * time.Second)
	s.removeExpired(c.read())
	if s.Len() != 1 {
		t.Fatal("cleanup removed an entry inside its grace period")
	}
	c.advance(time.Minute)
	s.removeExpired(c.read())
	if s.Len() != 0 {
		t.Fatal("cleanup kept an entry past its grace period")
	}
}

func TestStaleOnErrorRespectsDelete(t *testing.T) {
	s, c := newClockedStore[string, int](t, WithStaleOnError(time.Minute))
	var calls atomic.Int32
	s.Set("k", 1, time.Second)
	c.advance(2 * time.Second)
	s.Delete("k")
	if _, _, err := s.GetOrLoad(context.Background(), "k", time.Second, countingLoader(&calls, 0, false, errors.New("down"))); err == nil {
		t.Fatal("deleted value was served as stale")
	}
}

func TestRefreshAheadReloadsInBackground(t *testing.T) {
	s, c := newClockedStore[string, int](t, WithRefreshAhead(3*time.Second))
	s.Set("k", 1, 10*time.Second)
	c.advance(8 * time.Second)

	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	loader := func(context.Context, string) (int, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return 2, true, nil
	}
	for range 5 {
		if got, ok, err := s.GetOrLoad(context.Background(), "k", 10*time.Second, loader); got != 1 || !ok || err != nil {
			t.Fatalf("GetOrLoad = %v, %v, %v; want current value 1 while refreshing", got, ok, err)
		}
	}
	<-started
	close(release)
	waitUntil(t, "refreshed value", func() bool { v, _ := s.Get("k"); return v == 2 })
	if calls.Load() != 1 || s.Stats().Refreshes != 1 {
		t.Fatalf("calls = %d, stats = %+v; want one refresh", calls.Load(), s.Stats())
	}
}

func TestRefreshAheadRetriesAfterFailure(t *testing.T) {
	s, c := newClockedStore[string, int](t, WithRefreshAhead(3*time.Second))
	s.Set("k", 1, 10*time.Second)
	c.advance(8 * time.Second)
	var calls atomic.Int32
	failing := countingLoader(&calls, 0, false, errors.New("down"))
	s.GetOrLoad(context.Background(), "k", 10*time.Second, failing)
	waitUntil(t, "failed refresh", func() bool { return s.Stats().LoadErrors == 1 })
	waitUntil(t, "flight cleared", func() bool {
		sh := s.shardFor("k")
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		return sh.flights["k"] == nil
	})
	s.GetOrLoad(context.Background(), "k", 10*time.Second, failing)
	waitUntil(t, "retried refresh", func() bool { return calls.Load() == 2 })
	if got, _ := s.Get("k"); got != 1 {
		t.Fatalf("failed refresh replaced the value with %v", got)
	}
}

func TestRefreshAheadSkipsEntriesOutsideWindow(t *testing.T) {
	s, _ := newClockedStore[string, int](t, WithRefreshAhead(3*time.Second))
	var calls atomic.Int32
	s.Set("k", 1, 10*time.Second)
	s.Set("forever", 1, 0)
	s.GetOrLoad(context.Background(), "k", 10*time.Second, countingLoader(&calls, 2, true, nil))
	s.GetOrLoad(context.Background(), "forever", 0, countingLoader(&calls, 2, true, nil))
	if calls.Load() != 0 {
		t.Fatalf("refresh started outside the window; calls = %d", calls.Load())
	}
}

func TestClearRemovesEntriesAndInvalidatesLoads(t *testing.T) {
	s := MustNew[string, int](WithMaxCost(100, func(string, int) int64 { return 1 }))
	s.Set("a", 1, 0)
	s.Set("b", 2, 0)
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.GetOrLoad(context.Background(), "c", 0, func(context.Context, string) (int, bool, error) {
			close(started)
			<-release
			return 3, true, nil
		})
	}()
	<-started
	s.Clear()
	close(release)
	<-done
	if st := s.Stats(); st.Entries != 0 || st.Cost != 0 {
		t.Fatalf("stats after Clear = %+v", st)
	}
}

func TestWithNameRegistersStats(t *testing.T) {
	s := MustNew[string, int](WithName("test_users"))
	s.Set("k", 1, 0)
	s.Get("k")
	if _, err := New[string, int](WithName("test_users")); err == nil {
		t.Fatal("duplicate name accepted")
	}
	st, ok := AllStats()["test_users"].(Stats)
	if !ok || st.Hits != 1 || st.Entries != 1 {
		t.Fatalf("AllStats entry = %#v", AllStats()["test_users"])
	}
	PublishExpvar()
	PublishExpvar()
	if expvar.Get("go_store") == nil {
		t.Fatal("go_store expvar not published")
	}
	s.Close()
	if _, ok := AllStats()["test_users"]; ok {
		t.Fatal("Close did not release the name")
	}
	MustNew[string, int](WithName("test_users")).Close()
}

func TestExpireReplacesEntry(t *testing.T) {
	s, c := newClockedStore[string, int](t)
	s.Set("k", 1, time.Second)
	if !s.Expire("k", time.Minute) {
		t.Fatal("Expire failed on a live key")
	}
	c.advance(30 * time.Second)
	if got, ok := s.Get("k"); !ok || got != 1 {
		t.Fatalf("Get after Expire = %v, %v", got, ok)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTypedOptionsMustMatchStore(t *testing.T) {
	if _, err := New[string, int](WithMaxCost(10, func(string, string) int64 { return 1 })); err == nil {
		t.Fatal("cost function for another value type accepted")
	}
	if _, err := New[string, int](WithEvictionCallback(func(int, int, EvictionReason) {})); err == nil {
		t.Fatal("eviction callback for another key type accepted")
	}
}
