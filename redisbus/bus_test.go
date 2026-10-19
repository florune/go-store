package redisbus_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisv8 "github.com/go-redis/redis/v8"
	redisv9 "github.com/redis/go-redis/v9"

	store "github.com/florune/go-store"
	"github.com/florune/go-store/redisbus"
	"github.com/florune/go-store/redisbus/goredisv8"
	"github.com/florune/go-store/redisbus/goredisv9"
)

const channel = "test:l1:invalidate"

// client creates a Bus through one go-redis major version. Every test runs
// once per client.
type client struct {
	name   string
	newBus func(t *testing.T, addr string) *redisbus.Bus
}

var clients = []client{
	{"v9", func(t *testing.T, addr string) *redisbus.Bus {
		c := redisv9.NewClient(&redisv9.Options{Addr: addr, MaxRetries: -1})
		t.Cleanup(func() { c.Close() })
		bus, err := goredisv9.New(c, channel)
		if err != nil {
			t.Fatal(err)
		}
		return bus
	}},
	{"v8", func(t *testing.T, addr string) *redisbus.Bus {
		c := redisv8.NewClient(&redisv8.Options{Addr: addr, MaxRetries: -1})
		t.Cleanup(func() { c.Close() })
		bus, err := goredisv8.New(c, channel)
		if err != nil {
			t.Fatal(err)
		}
		return bus
	}},
}

func eachClient(t *testing.T, test func(t *testing.T, c client)) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) { test(t, c) })
	}
}

type uid int64

// instance is one server process: its own Redis client, Bus, and caches.
type instance struct {
	bus   *redisbus.Bus
	users *store.Store[uid, string]
	bind  *redisbus.StoreBinding[uid]
}

func newInstance(t *testing.T, mr *miniredis.Miniredis, c client) *instance {
	t.Helper()
	bus := c.newBus(t, mr.Addr())
	t.Cleanup(func() { bus.Close() })
	users := store.MustNew[uid, string]()
	t.Cleanup(users.Close)
	bind, err := redisbus.BindStore(bus, "user_info", users)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "subscription", func() bool { return bus.Stats().Resets >= 1 })
	return &instance{bus: bus, users: users, bind: bind}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func has(s *store.Store[uid, string], key uid) bool {
	_, ok := s.Get(key)
	return ok
}

func TestInvalidateReachesOtherInstances(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		a, b := newInstance(t, mr, c), newInstance(t, mr, c)
		for _, in := range []*instance{a, b} {
			in.users.Set(1, "ada", time.Hour)
			in.users.Set(2, "grace", time.Hour)
		}

		if err := a.bind.Invalidate(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if has(a.users, 1) {
			t.Fatal("Invalidate did not delete the local key")
		}
		waitUntil(t, "remote delete", func() bool { return !has(b.users, 1) })
		if !has(b.users, 2) {
			t.Fatal("remote instance lost an unrelated key")
		}
		time.Sleep(20 * time.Millisecond)
		if st := a.bus.Stats(); st.Published != 1 || st.Received != 0 {
			t.Fatalf("sender stats = %+v; it must ignore its own message", st)
		}
		if st := b.bus.Stats(); st.Received != 1 {
			t.Fatalf("receiver stats = %+v", st)
		}
	})
}

func TestInvalidateAll(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		a, b := newInstance(t, mr, c), newInstance(t, mr, c)
		b.users.Set(1, "ada", time.Hour)
		b.users.Set(2, "grace", time.Hour)
		if err := a.bind.InvalidateAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitUntil(t, "remote clear", func() bool { return b.users.Len() == 0 })
	})
}

func TestPublishOnlyBusForWriters(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		server := newInstance(t, mr, c)
		server.users.Set(42, "ada", time.Hour)

		admin := c.newBus(t, mr.Addr())
		defer admin.Close()
		if err := admin.Publish(context.Background(), "user_info", "42"); err != nil {
			t.Fatal(err)
		}
		waitUntil(t, "delete from admin", func() bool { return !has(server.users, 42) })
		if n := mr.PubSubNumSub(channel)[channel]; n != 1 {
			t.Fatalf("subscribers = %d; a publish-only bus must not subscribe", n)
		}
	})
}

func TestBindSnapshot(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		a, b := newInstance(t, mr, c), newInstance(t, mr, c)
		var version atomic.Int32
		newSnapshot := func() *store.Snapshot[int32] {
			s := store.MustNewSnapshot(func(context.Context) (int32, error) { return version.Load(), nil },
				store.WithMaxAge(0))
			t.Cleanup(s.Close)
			return s
		}
		snapA, snapB := newSnapshot(), newSnapshot()
		bindA, err := redisbus.BindSnapshot(a.bus, "game_lib", snapA)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := redisbus.BindSnapshot(b.bus, "game_lib", snapB); err != nil {
			t.Fatal(err)
		}
		snapA.Get(context.Background())
		snapB.Get(context.Background())

		version.Store(2)
		if err := bindA.Invalidate(context.Background()); err != nil {
			t.Fatal(err)
		}
		for name, s := range map[string]*store.Snapshot[int32]{"local": snapA, "remote": snapB} {
			waitUntil(t, name+" reload", func() bool { v, _ := s.Get(context.Background()); return v == 2 })
		}
	})
}

func TestResubscribeClearsBoundCaches(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		in := newInstance(t, mr, c)
		in.users.Set(1, "ada", time.Hour)

		mr.Close() // invalidations published now would be lost
		if err := mr.Restart(); err != nil {
			t.Fatal(err)
		}
		waitUntil(t, "resubscription", func() bool { return in.bus.Stats().Resets >= 2 })
		if has(in.users, 1) {
			t.Fatal("cache kept a key across a subscription gap")
		}
	})
}

func TestDropsBadMessages(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		in := newInstance(t, mr, c)
		in.users.Set(1, "ada", time.Hour)
		for _, payload := range []string{
			"not json",
			`{"from":"x","ns":"unknown","keys":["1"]}`,
			`{"from":"x","ns":"user_info","keys":["not-a-number"]}`,
		} {
			mr.Publish(channel, payload)
		}
		waitUntil(t, "drops", func() bool { return in.bus.Stats().Dropped == 3 })
		if !has(in.users, 1) {
			t.Fatal("a bad message deleted a key")
		}
	})
}

func TestKeyTypes(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		sender, receiver := newInstance(t, mr, c), newInstance(t, mr, c)

		type code string
		codes := store.MustNew[code, int]()
		defer codes.Close()
		codes.Set("vip", 1, time.Hour)
		bindCodes, err := redisbus.BindStore(sender.bus, "codes", store.MustNew[code, int]())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := redisbus.BindStore(receiver.bus, "codes", codes); err != nil {
			t.Fatal(err)
		}

		ids := store.MustNew[uint32, int]()
		defer ids.Close()
		ids.Set(7, 1, time.Hour)
		bindIDs, err := redisbus.BindStore(sender.bus, "ids", store.MustNew[uint32, int]())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := redisbus.BindStore(receiver.bus, "ids", ids); err != nil {
			t.Fatal(err)
		}

		bindCodes.Invalidate(context.Background(), "vip")
		bindIDs.Invalidate(context.Background(), 7)
		waitUntil(t, "string-kind key delete", func() bool { _, ok := codes.Get("vip"); return !ok })
		waitUntil(t, "uint key delete", func() bool { _, ok := ids.Get(7); return !ok })

		type pair struct{ a, b int }
		if _, err := redisbus.BindStore(sender.bus, "pairs", store.MustNew[pair, int]()); err == nil {
			t.Fatal("struct key type accepted")
		}
	})
}

func TestBindValidation(t *testing.T) {
	eachClient(t, func(t *testing.T, c client) {
		mr := miniredis.RunT(t)
		in := newInstance(t, mr, c)
		if _, err := redisbus.BindStore(in.bus, "user_info", store.MustNew[uid, string]()); err == nil {
			t.Fatal("duplicate namespace accepted")
		}
		if _, err := redisbus.BindStore(in.bus, "", store.MustNew[uid, string]()); err == nil {
			t.Fatal("empty namespace accepted")
		}
		in.bus.Close()
		if _, err := redisbus.BindStore(in.bus, "late", store.MustNew[uid, string]()); err == nil {
			t.Fatal("bind after Close accepted")
		}
	})
}

func TestNewRejectsNil(t *testing.T) {
	if _, err := redisbus.New(nil, channel); err == nil {
		t.Fatal("nil conn accepted")
	}
	if _, err := goredisv9.New(nil, channel); err == nil {
		t.Fatal("nil v9 client accepted")
	}
	if _, err := goredisv8.New(nil, channel); err == nil {
		t.Fatal("nil v8 client accepted")
	}
	c := redisv9.NewClient(&redisv9.Options{Addr: "localhost:0"})
	defer c.Close()
	if _, err := goredisv9.New(c, ""); err == nil {
		t.Fatal("empty channel accepted")
	}
}

// A process on go-redis v8 and one on v9 share a channel.
func TestMixedClientVersions(t *testing.T) {
	mr := miniredis.RunT(t)
	a, b := newInstance(t, mr, clients[0]), newInstance(t, mr, clients[1])
	a.users.Set(1, "ada", time.Hour)
	b.users.Set(1, "ada", time.Hour)
	if err := a.bind.Invalidate(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "v8 delete from v9", func() bool { return !has(b.users, 1) })
	a.users.Set(2, "grace", time.Hour)
	if err := b.bind.Invalidate(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "v9 delete from v8", func() bool { return !has(a.users, 2) })
}
