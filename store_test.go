package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/florune/go-store"
)

type user struct {
	Name string
	Age  int
}

func TestTypedStoreAndExpiration(t *testing.T) {
	s := store.MustNew[string, user](store.WithCleanupInterval(time.Millisecond))
	defer s.Close()
	want := user{Name: "Ada", Age: 37}
	if !s.Set("user:1", want, 5*time.Millisecond) {
		t.Fatal("Set rejected")
	}
	if got, ok := s.Get("user:1"); !ok || got != want {
		t.Fatalf("Get = %#v, %v", got, ok)
	}
	time.Sleep(10 * time.Millisecond)
	if _, ok := s.Get("user:1"); ok {
		t.Fatal("expired value remained visible")
	}
}

func TestSetNXCompareAndSwapAndAdd(t *testing.T) {
	s := store.MustNew[string, int]()
	if !s.SetNX("n", 1, 0) || s.SetNX("n", 2, 0) {
		t.Fatal("SetNX semantics broken")
	}
	if !store.CompareAndSwap(s, "n", 1, 4, 0) {
		t.Fatal("CAS failed")
	}
	if got := store.Add(s, "n", 3, 0); got != 7 {
		t.Fatalf("Add = %d", got)
	}
}

func TestGetOrLoadCoalesces(t *testing.T) {
	s := store.MustNew[string, string]()
	var calls atomic.Int32
	loader := func(context.Context, string) (string, bool, error) {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return "value", true, nil
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, ok, err := s.GetOrLoad(context.Background(), "key", time.Minute, loader)
			if err != nil || !ok || got != "value" {
				t.Errorf("load = %q, %v, %v", got, ok, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("loader called %d times", calls.Load())
	}
}

func TestCapacity(t *testing.T) {
	s := store.MustNew[string, string](store.WithMaxCost(2, func(_, _ string) int64 { return 1 }))
	for _, key := range []string{"a", "b", "c"} {
		s.Set(key, key, 0)
	}
	if got := s.Stats(); got.Cost > 2 || got.Entries > 2 || got.Evictions == 0 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestBytesOwnership(t *testing.T) {
	b, err := store.NewBytes[string]()
	if err != nil {
		t.Fatal(err)
	}
	source := []byte("abc")
	b.Set("k", source, 0)
	source[0] = 'x'
	got, _ := b.Get("k")
	got[1] = 'x'
	again, _ := b.Get("k")
	if string(again) != "abc" {
		t.Fatalf("stored bytes mutated: %q", again)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := store.MustNew[int, int]()
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				key := (worker*2000 + i) % 128
				s.Set(key, i, time.Second)
				s.Get(key)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkStoreParallel(b *testing.B) {
	s := store.MustNew[int, int]()
	for i := range 1024 {
		s.Set(i, i, 0)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(i & 1023)
			i++
		}
	})
}

func BenchmarkGetOrLoadHitParallel(b *testing.B) {
	s := store.MustNew[int, int](store.WithRefreshAhead(time.Second))
	for i := range 1024 {
		s.Set(i, i, time.Hour)
	}
	loader := func(_ context.Context, key int) (int, bool, error) { return key, true, nil }
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.GetOrLoad(ctx, i&1023, time.Hour, loader)
			i++
		}
	})
}
