package redisbus_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	store "github.com/florune/go-store"
	"github.com/florune/go-store/redisbus"
)

const channel = "test:l1:invalidate"

type uid int64

// instance is one server process: its own Redis client, Bus, and caches.
type instance struct {
	bus   *redisbus.Bus
	users *store.Store[uid, string]
	bind  *redisbus.StoreBinding[uid]
}

func newInstance(t *testing.T, mr *miniredis.Miniredis) *instance {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { client.Close() })
	bus, err := redisbus.New(client, channel)
	if err != nil {
		t.Fatal(err)
	}
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
	mr := miniredis.RunT(t)
	a, b := newInstance(t, mr), newInstance(t, mr)
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
}

func TestInvalidateAll(t *testing.T) {
	mr := miniredis.RunT(t)
	a, b := newInstance(t, mr), newInstance(t, mr)
	b.users.Set(1, "ada", time.Hour)
	b.users.Set(2, "grace", time.Hour)
	if err := a.bind.InvalidateAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "remote clear", func() bool { return b.users.Len() == 0 })
}

func TestPublishOnlyBusForWriters(t *testing.T) {
	mr := miniredis.RunT(t)
	server := newInstance(t, mr)
	server.users.Set(42, "ada", time.Hour)

	adminClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer adminClient.Close()
	admin, err := redisbus.New(adminClient, channel)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.Publish(context.Background(), "user_info", "42"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "delete from admin", func() bool { return !has(server.users, 42) })
	if n := mr.PubSubNumSub(channel)[channel]; n != 1 {
		t.Fatalf("subscribers = %d; a publish-only bus must not subscribe", n)
	}
}

func TestBindSnapshot(t *testing.T) {
	mr := miniredis.RunT(t)
	a, b := newInstance(t, mr), newInstance(t, mr)
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
}

func TestResubscribeClearsBoundCaches(t *testing.T) {
	mr := miniredis.RunT(t)
	in := newInstance(t, mr)
	in.users.Set(1, "ada", time.Hour)

	mr.Close() // invalidations published now would be lost
	if err := mr.Restart(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "resubscription", func() bool { return in.bus.Stats().Resets >= 2 })
	if has(in.users, 1) {
		t.Fatal("cache kept a key across a subscription gap")
	}
}

func TestDropsBadMessages(t *testing.T) {
	mr := miniredis.RunT(t)
	in := newInstance(t, mr)
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
}

func TestKeyTypes(t *testing.T) {
	mr := miniredis.RunT(t)
	sender, receiver := newInstance(t, mr), newInstance(t, mr)

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
}

func TestBindValidation(t *testing.T) {
	mr := miniredis.RunT(t)
	in := newInstance(t, mr)
	if _, err := redisbus.BindStore(in.bus, "user_info", store.MustNew[uid, string]()); err == nil {
		t.Fatal("duplicate namespace accepted")
	}
	if _, err := redisbus.BindStore(in.bus, "", store.MustNew[uid, string]()); err == nil {
		t.Fatal("empty namespace accepted")
	}
	if _, err := redisbus.New(nil, channel); err == nil {
		t.Fatal("nil client accepted")
	}
	in.bus.Close()
	if _, err := redisbus.BindStore(in.bus, "late", store.MustNew[uid, string]()); err == nil {
		t.Fatal("bind after Close accepted")
	}
}
