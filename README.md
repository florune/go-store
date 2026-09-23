# go-store

`go-store` is a typed, in-process L1 store for Go services that need to avoid
unnecessary Redis or database round trips. It combines sharded locking, per-key
TTL, request-coalesced loading, and optional cost-bounded eviction without
serializing application values.

Requires Go 1.24 or newer.

## Quick start

```go
package main

import (
	"context"
	"time"

	store "github.com/florune/go-store"
)

type User struct {
	ID   int64
	Name string
}

func main() {
	users := store.MustNew[string, User](
		store.WithCleanupInterval[string, User](time.Second),
	)
	defer users.Close()

	users.Set("user:42", User{ID: 42, Name: "Ada"}, time.Minute)
	user, ok := users.Get("user:42")
	_, _ = user, ok

	user, ok, err := users.GetOrLoad(
		context.Background(),
		"user:43",
		time.Minute,
		func(ctx context.Context, key string) (User, bool, error) {
			// Load from Redis or a database. Concurrent misses for this key
			// share this one call.
			return User{ID: 43, Name: "Grace"}, true, nil
		},
	)
	_, _, _ = user, ok, err
}
```

## Included structures

- `Store[K, V]`: generic key/value storage, TTL, `SetNX`, statistics, and
  coalesced `GetOrLoad`.
- `Bytes[K]`: byte storage with defensive copies plus an explicitly unsafe
  zero-copy read view.
- `Hash[K, F, V]`: Redis-style typed field/value maps with a TTL per top-level
  key.
- `Set[K, M]`: Redis-style typed member sets with a TTL per top-level key.
- `CompareAndSwap` and `Add`: atomic helpers for comparable and integer values.

## Capacity control

Use `WithMaxCost` to bound a store by an application-defined cost. When the
limit is exceeded, the store samples entries across its shards and removes the
least recently accessed candidate.

```go
cache := store.MustNew[string, User](
	store.WithMaxCost[string, User](10_000, func(_ string, _ User) int64 {
		return 1
	}),
)
```

This is intentionally approximate: it avoids a global lock and the write
amplification of an exact LRU list.

## Value ownership

Generic values follow normal Go assignment semantics. A struct value itself is
copied, but maps, pointers, slices, and reference-bearing fields still share
their backing data. Treat such values as immutable after insertion or provide
your own synchronization. `Bytes` performs defensive copies on ordinary reads
and writes.

`GetUnsafeView` returns the store-owned byte slice without copying. Never
modify it. A later replacement or deletion may make the view stale, so do not
retain it longer than necessary.

## TTL and cleanup

A TTL greater than zero sets an expiry. Zero or negative TTL means the entry
does not expire. Expired values are never returned. They are removed lazily on
access and, when `WithCleanupInterval` is configured, by a background pass.
Call `Close` to stop that background goroutine.

## Concurrency notes

The top-level store is split into independently locked shards. Reads take a
shard read lock; recency updates are atomic. Capacity eviction is sampled and
does not maintain a globally locked LRU list. Eviction callbacks are called
synchronously after internal locks are released, so callbacks should return
quickly.

No `unsafe` implementation is enabled: the standard-library comparable-key
hash added in Go 1.24 provides safe generic sharding, and current benchmarks do
not justify runtime-dependent memory tricks.

## Development

```sh
go test ./...
go test -race ./...
go test -bench=. -benchmem ./...
go vet ./...
```

## License

Apache-2.0.
