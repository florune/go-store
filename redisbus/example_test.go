package redisbus_test

import (
	"context"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	store "github.com/florune/go-store"
	"github.com/florune/go-store/redisbus"
)

// Two server processes share one channel; an update on one invalidates the
// cached user on both.
func ExampleBindStore() {
	mr, _ := miniredis.Run() // stands in for the shared Redis
	defer mr.Close()

	newProcess := func() (*store.Store[int64, string], *redisbus.StoreBinding[int64], func()) {
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		bus, _ := redisbus.New(client, "app:l1:invalidate")
		users := store.MustNew[int64, string]()
		binding, _ := redisbus.BindStore(bus, "user_info", users)
		for bus.Stats().Resets == 0 { // wait for the subscription
			time.Sleep(time.Millisecond)
		}
		return users, binding, func() { bus.Close(); users.Close(); client.Close() }
	}
	usersA, bindingA, closeA := newProcess()
	defer closeA()
	usersB, _, closeB := newProcess()
	defer closeB()

	usersA.Set(42, "ada", time.Hour)
	usersB.Set(42, "ada", time.Hour)

	// Process A updates user 42 in the database and Redis, then:
	bindingA.Invalidate(context.Background(), 42)

	for {
		if _, ok := usersB.Get(42); !ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	_, inA := usersA.Get(42)
	_, inB := usersB.Get(42)
	fmt.Println(inA, inB)
	// Output:
	// false false
}

// A writer that holds no cache, such as an admin service, only publishes.
func ExampleBus_Publish() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	admin, _ := redisbus.New(client, "app:l1:invalidate")
	defer admin.Close()

	ctx := context.Background()
	fmt.Println(admin.Publish(ctx, "user_info", "42")) // after banning user 42
	fmt.Println(admin.PublishAll(ctx, "game_lib"))     // after editing the game table
	// Output:
	// <nil>
	// <nil>
}
