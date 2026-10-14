package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	store "github.com/florune/go-store"
)

// blockingLoader returns a loader that signals started, waits for release,
// and then returns value.
func blockingLoader(value int, started, release chan struct{}) store.Loader[string, int] {
	return func(context.Context, string) (int, bool, error) {
		close(started)
		<-release
		return value, true, nil
	}
}

func constLoader(value int) store.Loader[string, int] {
	return func(context.Context, string) (int, bool, error) { return value, true, nil }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGetOrLoadRecoversLoaderPanic(t *testing.T) {
	s := store.MustNew[string, int]()
	cause := errors.New("boom")
	_, ok, err := s.GetOrLoad(context.Background(), "k", 0, func(context.Context, string) (int, bool, error) {
		panic(cause)
	})
	var panicErr *store.LoaderPanicError
	if ok || !errors.As(err, &panicErr) || !errors.Is(err, cause) || len(panicErr.Stack) == 0 {
		t.Fatalf("GetOrLoad = ok %v, err %v; want LoaderPanicError wrapping cause", ok, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, ok, err := s.GetOrLoad(ctx, "k", 0, constLoader(1))
	if err != nil || !ok || got != 1 {
		t.Fatalf("GetOrLoad after panic = %v, %v, %v", got, ok, err)
	}
	if st := s.Stats(); st.Loads != 2 || st.LoadErrors != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestGetOrLoadDiscardsResultInvalidatedDuringLoad(t *testing.T) {
	writes := map[string]func(*store.Store[string, int]){
		"Delete": func(s *store.Store[string, int]) { s.Delete("k") },
		"Set":    func(s *store.Store[string, int]) { s.Set("k", 2, 0) },
		"SetNX":  func(s *store.Store[string, int]) { s.SetNX("k", 2, 0) },
		"Add":    func(s *store.Store[string, int]) { store.Add(s, "k", 2, 0) },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			s := store.MustNew[string, int]()
			started, release := make(chan struct{}), make(chan struct{})
			result := make(chan int)
			go func() {
				got, _, _ := s.GetOrLoad(context.Background(), "k", 0, blockingLoader(1, started, release))
				result <- got
			}()
			<-started
			write(s)
			want, wantOK := s.Get("k")
			close(release)
			if got := <-result; got != 1 {
				t.Fatalf("caller got %d, want the loaded value 1", got)
			}
			if got, ok := s.Get("k"); got != want || ok != wantOK {
				t.Fatalf("Get after load = %v, %v; want %v, %v (stale load must not overwrite)", got, ok, want, wantOK)
			}
		})
	}
}

func TestGetOrLoadLeaderCancelDoesNotFailWaiters(t *testing.T) {
	s := store.MustNew[string, int]()
	started, release := make(chan struct{}), make(chan struct{})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error)
	go func() {
		_, _, err := s.GetOrLoad(leaderCtx, "k", 0, blockingLoader(1, started, release))
		leaderErr <- err
	}()
	<-started

	type result struct {
		value int
		ok    bool
		err   error
	}
	waiter := make(chan result)
	go func() {
		value, ok, err := s.GetOrLoad(context.Background(), "k", 0, func(context.Context, string) (int, bool, error) {
			t.Error("waiter started a second load")
			return 0, false, nil
		})
		waiter <- result{value, ok, err}
	}()
	waitFor(t, "waiter to join", func() bool { return s.Stats().Misses == 2 })

	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader err = %v, want context.Canceled", err)
	}
	close(release)
	if got := <-waiter; got.err != nil || !got.ok || got.value != 1 {
		t.Fatalf("waiter = %+v", got)
	}
	if got, ok := s.Get("k"); !ok || got != 1 {
		t.Fatalf("Get = %v, %v; want the loaded value cached", got, ok)
	}
}

func TestGetOrLoadCancelsLoaderWhenAllCallersLeave(t *testing.T) {
	s := store.MustNew[string, int]()
	started, loaderCanceled := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error)
	go func() {
		_, _, err := s.GetOrLoad(ctx, "k", 0, func(ctx context.Context, _ string) (int, bool, error) {
			close(started)
			<-ctx.Done()
			close(loaderCanceled)
			return 1, true, nil
		})
		errc <- err
	}()
	<-started
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	select {
	case <-loaderCanceled:
	case <-time.After(time.Second):
		t.Fatal("loader context was not canceled after its only caller left")
	}

	got, ok, err := s.GetOrLoad(context.Background(), "k", 0, constLoader(2))
	if err != nil || !ok || got != 2 {
		t.Fatalf("next GetOrLoad = %v, %v, %v; want a fresh load", got, ok, err)
	}
	if got, _ := s.Get("k"); got != 2 {
		t.Fatalf("Get = %v; abandoned load must not overwrite the fresh one", got)
	}
}

func TestGetOrLoadKeepsLoaderContextValues(t *testing.T) {
	type ctxKey struct{}
	s := store.MustNew[string, string]()
	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-1")
	got, _, _ := s.GetOrLoad(ctx, "k", 0, func(ctx context.Context, _ string) (string, bool, error) {
		v, _ := ctx.Value(ctxKey{}).(string)
		return v, true, nil
	})
	if got != "trace-1" {
		t.Fatalf("loader saw context value %q", got)
	}
}

func TestGetOrLoadCanceledContextSkipsLoad(t *testing.T) {
	s := store.MustNew[string, int]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := s.GetOrLoad(ctx, "k", 0, func(context.Context, string) (int, bool, error) {
		t.Error("loader called with an already canceled context")
		return 0, false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetOrLoadStats(t *testing.T) {
	s := store.MustNew[string, int]()
	s.GetOrLoad(context.Background(), "k", 0, constLoader(1))
	s.GetOrLoad(context.Background(), "k", 0, constLoader(1))
	if st := s.Stats(); st.Hits != 1 || st.Misses != 1 || st.Loads != 1 {
		t.Fatalf("stats = %+v; want one miss, one load, one hit", st)
	}
}

func TestGetOrLoadReturnsValueTooCostlyToStore(t *testing.T) {
	s := store.MustNew[string, string](store.WithMaxCost(3, func(_, v string) int64 {
		return int64(len(v))
	}))
	got, ok, err := s.GetOrLoad(context.Background(), "k", 0, func(context.Context, string) (string, bool, error) {
		return "too long", true, nil
	})
	if err != nil || !ok || got != "too long" {
		t.Fatalf("GetOrLoad = %q, %v, %v", got, ok, err)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("value larger than MaxCost was stored")
	}
}
