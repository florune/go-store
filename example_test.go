package store_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	store "github.com/florune/go-store"
)

type User struct {
	ID   int64
	Name string
}

var errNotFound = errors.New("not found")

// findUser stands in for a Redis or database lookup.
func findUser(_ context.Context, id int64) (User, error) {
	if id == 404 {
		return User{}, errNotFound
	}
	return User{ID: id, Name: fmt.Sprint("user-", id)}, nil
}

// A read-through cache in front of Redis or a database: concurrent misses for
// one key share one lookup, missing users are cached briefly, and a failed
// reload falls back to the previous value.
func ExampleStore_GetOrLoad() {
	users := store.MustNew[int64, User](
		store.WithName("example_users"),
		store.WithMaxCost(100_000, func(int64, User) int64 { return 1 }),
		store.WithNegativeTTL(30*time.Second),
		store.WithLoadTimeout(2*time.Second),
		store.WithStaleOnError(time.Minute),
		store.WithTTLJitter(0.1),
	)
	defer users.Close()

	load := func(ctx context.Context, id int64) (User, bool, error) {
		u, err := findUser(ctx, id)
		if errors.Is(err, errNotFound) {
			return User{}, false, nil // cached as absent for 30s
		}
		return u, err == nil, err // errors are never cached
	}

	ctx := context.Background()
	u, ok, err := users.GetOrLoad(ctx, 42, 5*time.Minute, load)
	fmt.Println(u.Name, ok, err)
	_, ok, err = users.GetOrLoad(ctx, 404, 5*time.Minute, load)
	fmt.Println(ok, err)

	// After the user changes in the database (and Redis):
	users.Delete(42)
	// Output:
	// user-42 true <nil>
	// false <nil>
}

// A configuration table read on every request: Get serves it from memory,
// and the table is reloaded only when its version changes.
func ExampleSnapshot() {
	version := "v1"
	gameLib := store.MustNewSnapshot(
		func(context.Context) ([]string, error) {
			return []string{"slots", "poker"}, nil // e.g. GET and decode a JSON table
		},
		store.WithName("example_game_lib"),
		store.WithVersion(func(context.Context) (string, error) {
			return version, nil // e.g. GET a small hash of the table
		}),
	)
	defer gameLib.Close()

	games, err := gameLib.Get(context.Background()) // shared value: read-only
	fmt.Println(games, err)
	// Output:
	// [slots poker] <nil>
}

// Caching "not found" keeps lookups of the common negative case, such as
// "this account is not banned", away from Redis.
func ExampleWithNegativeTTL() {
	banned := store.MustNew[int64, struct{}](store.WithNegativeTTL(30 * time.Second))
	defer banned.Close()

	isBanned := func(ctx context.Context, uid int64) bool {
		_, ok, _ := banned.GetOrLoad(ctx, uid, time.Minute,
			func(context.Context, int64) (struct{}, bool, error) {
				return struct{}{}, false, nil // e.g. GET ban key: redis.Nil
			})
		return ok
	}
	ctx := context.Background()
	fmt.Println(isBanned(ctx, 7), isBanned(ctx, 7))
	fmt.Println(banned.Stats().Loads, banned.Stats().NegativeHits)
	// Output:
	// false false
	// 1 1
}

// Set with a short TTL as a local write throttle: refresh a shared online
// marker at most once per interval per user.
func ExampleStore_SetNX() {
	recent := store.MustNew[int64, struct{}]()
	defer recent.Close()

	writes := 0
	touch := func(uid int64) {
		if recent.SetNX(uid, struct{}{}, 30*time.Second) {
			writes++ // e.g. ZADD online_users
		}
	}
	for range 5 {
		touch(1)
	}
	fmt.Println(writes)
	// Output:
	// 1
}
