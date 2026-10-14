package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// source is a fake config table with a version, as admin publishes to Redis.
type source struct {
	mu        sync.Mutex
	value     int
	version   string
	loadErr   error
	verErr    error
	loads     atomic.Int32
	loadDelay chan struct{} // if set, each load waits for a receive
}

func (src *source) set(value int, version string) {
	src.mu.Lock()
	src.value, src.version = value, version
	src.mu.Unlock()
}

func (src *source) fail(loadErr, verErr error) {
	src.mu.Lock()
	src.loadErr, src.verErr = loadErr, verErr
	src.mu.Unlock()
}

func (src *source) load(ctx context.Context) (int, error) {
	src.loads.Add(1)
	if src.loadDelay != nil {
		select {
		case <-src.loadDelay:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	return src.value, src.loadErr
}

func (src *source) currentVersion(context.Context) (string, error) {
	src.mu.Lock()
	defer src.mu.Unlock()
	return src.version, src.verErr
}

func newTestSnapshot(t *testing.T, src *source, options ...SnapshotOption) *Snapshot[int] {
	t.Helper()
	s := MustNewSnapshot(src.load, options...)
	t.Cleanup(s.Close)
	return s
}

func mustGet(t *testing.T, s *Snapshot[int]) int {
	t.Helper()
	v, err := s.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return v
}

func TestSnapshotLoadsLazilyOnce(t *testing.T) {
	src := &source{value: 1, loadDelay: make(chan struct{})}
	s := newTestSnapshot(t, src)
	if src.loads.Load() != 0 {
		t.Fatal("snapshot loaded before first Get")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v := mustGet(t, s); v != 1 {
				t.Errorf("Get = %d", v)
			}
		}()
	}
	waitUntil(t, "first load to start", func() bool { return src.loads.Load() == 1 })
	close(src.loadDelay)
	wg.Wait()
	mustGet(t, s)
	if src.loads.Load() != 1 {
		t.Fatalf("loads = %d, want 1", src.loads.Load())
	}
}

func TestSnapshotFirstLoadErrorIsRetried(t *testing.T) {
	src := &source{value: 1}
	src.fail(errors.New("redis down"), nil)
	s := newTestSnapshot(t, src)
	if _, err := s.Get(context.Background()); err == nil {
		t.Fatal("Get succeeded without a value")
	}
	src.fail(nil, nil)
	if v := mustGet(t, s); v != 1 {
		t.Fatalf("Get after recovery = %d", v)
	}
}

func TestSnapshotReloadsOnlyWhenVersionChanges(t *testing.T) {
	src := &source{value: 1, version: "v1"}
	s := newTestSnapshot(t, src, WithVersion(src.currentVersion), WithPollInterval(5*time.Millisecond))
	mustGet(t, s)
	waitUntil(t, "a few version checks", func() bool { return s.Stats().VersionChecks >= 4 })
	if src.loads.Load() != 1 {
		t.Fatalf("reloaded with unchanged version; loads = %d", src.loads.Load())
	}
	src.set(2, "v2")
	waitUntil(t, "reload after version change", func() bool { return mustGet(t, s) == 2 })
	if st := s.Stats(); st.Version != "v2" || !st.Loaded || st.LoadedAt.IsZero() {
		t.Fatalf("stats = %+v", st)
	}
}

func TestSnapshotKeepsValueWhenReloadFails(t *testing.T) {
	src := &source{value: 1, version: "v1"}
	s := newTestSnapshot(t, src, WithVersion(src.currentVersion), WithPollInterval(5*time.Millisecond))
	mustGet(t, s)

	src.set(2, "v2")
	src.fail(errors.New("load failed"), nil)
	waitUntil(t, "failed reload", func() bool { return s.Stats().LoadErrors >= 1 })
	if v := mustGet(t, s); v != 1 {
		t.Fatalf("Get = %d; want previous value while reloads fail", v)
	}

	src.fail(nil, errors.New("version failed"))
	before := s.Stats().LoadErrors
	waitUntil(t, "failed version check", func() bool { return s.Stats().LoadErrors > before })
	if v := mustGet(t, s); v != 1 {
		t.Fatalf("Get = %d; want previous value while version checks fail", v)
	}

	src.fail(nil, nil)
	waitUntil(t, "recovery", func() bool { return mustGet(t, s) == 2 })
}

func TestSnapshotMaxAgeWithoutVersion(t *testing.T) {
	src := &source{value: 1}
	s := newTestSnapshot(t, src, WithMaxAge(10*time.Millisecond))
	mustGet(t, s)
	src.set(2, "")
	waitUntil(t, "max-age reload", func() bool { return mustGet(t, s) == 2 })
}

func TestSnapshotMaxAgeWithUnchangedVersion(t *testing.T) {
	src := &source{value: 1, version: "v1"}
	s := newTestSnapshot(t, src,
		WithVersion(src.currentVersion), WithPollInterval(5*time.Millisecond), WithMaxAge(20*time.Millisecond))
	mustGet(t, s)
	src.set(2, "v1") // data changed without a version bump
	waitUntil(t, "max-age reload", func() bool { return mustGet(t, s) == 2 })
}

func TestSnapshotInvalidate(t *testing.T) {
	src := &source{value: 1, version: "v1"}
	s := newTestSnapshot(t, src, WithVersion(src.currentVersion), WithPollInterval(time.Hour))
	s.Invalidate() // before first Get: nothing to reload
	if src.loads.Load() != 0 {
		t.Fatal("Invalidate loaded before first Get")
	}
	mustGet(t, s)
	src.set(2, "v1")
	s.Invalidate()
	waitUntil(t, "forced reload", func() bool { return mustGet(t, s) == 2 })
}

func TestSnapshotInvalidateDuringLoadReloadsAgain(t *testing.T) {
	src := &source{value: 1, loadDelay: make(chan struct{})}
	s := newTestSnapshot(t, src, WithMaxAge(0))
	got := make(chan int)
	go func() { got <- mustGet(t, s) }()
	waitUntil(t, "first load to start", func() bool { return src.loads.Load() == 1 })
	src.set(2, "") // the in-flight load may have read the old data
	s.Invalidate()
	src.loadDelay <- struct{}{}
	<-got
	src.loadDelay <- struct{}{}
	waitUntil(t, "second load", func() bool { return mustGet(t, s) == 2 })
}

func TestSnapshotRecoversPanic(t *testing.T) {
	var calls atomic.Int32
	s := MustNewSnapshot(func(context.Context) (int, error) {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return 1, nil
	})
	defer s.Close()
	_, err := s.Get(context.Background())
	var panicErr *LoaderPanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("err = %v; want LoaderPanicError", err)
	}
	if v := mustGet(t, s); v != 1 {
		t.Fatalf("Get after panic = %d", v)
	}
}

func TestSnapshotLoadTimeout(t *testing.T) {
	src := &source{loadDelay: make(chan struct{})}
	s := newTestSnapshot(t, src, WithLoadTimeout(10*time.Millisecond))
	if _, err := s.Get(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want DeadlineExceeded", err)
	}
}

func TestSnapshotGetHonorsContext(t *testing.T) {
	src := &source{value: 1, loadDelay: make(chan struct{})}
	s := newTestSnapshot(t, src)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Get(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want Canceled", err)
	}
	close(src.loadDelay)
	waitUntil(t, "background load", func() bool { return s.Stats().Loaded })
	if v := mustGet(t, s); v != 1 {
		t.Fatalf("Get = %d", v)
	}
}

func TestSnapshotNameAndClose(t *testing.T) {
	src := &source{value: 1, version: "v1"}
	s := MustNewSnapshot(src.load, WithName("test_game_lib"), WithVersion(src.currentVersion), WithPollInterval(time.Millisecond))
	mustGet(t, s)
	if _, err := New[string, int](WithName("test_game_lib")); err == nil {
		t.Fatal("store reused a snapshot's name")
	}
	if st, ok := AllStats()["test_game_lib"].(SnapshotStats); !ok || st.Loads != 1 {
		t.Fatalf("AllStats entry = %#v", AllStats()["test_game_lib"])
	}
	s.Close()
	checks := s.Stats().VersionChecks
	time.Sleep(10 * time.Millisecond)
	if s.Stats().VersionChecks != checks {
		t.Fatal("version polling continued after Close")
	}
	if v := mustGet(t, s); v != 1 {
		t.Fatalf("Get after Close = %d", v)
	}
	if _, ok := AllStats()["test_game_lib"]; ok {
		t.Fatal("Close did not release the name")
	}
}

func TestSnapshotOptionValidation(t *testing.T) {
	if _, err := NewSnapshot[int](nil); err == nil {
		t.Fatal("nil load accepted")
	}
	load := func(context.Context) (int, error) { return 0, nil }
	for _, option := range []SnapshotOption{WithVersion(nil), WithPollInterval(0), WithMaxAge(-1), WithName("")} {
		if _, err := NewSnapshot(load, option); err == nil {
			t.Fatal("invalid option accepted")
		}
	}
}

func BenchmarkSnapshotGetParallel(b *testing.B) {
	s := MustNewSnapshot(func(context.Context) ([]int, error) { return make([]int, 1000), nil })
	defer s.Close()
	ctx := context.Background()
	s.Get(ctx)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get(ctx)
		}
	})
}
