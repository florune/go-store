# go-store

`go-store` is a typed, in-process L1 store for Go services that need to avoid
unnecessary Redis or database round trips. It combines sharded locking, per-key
TTL, request-coalesced loading, and optional cost-bounded eviction without
serializing application values. `Snapshot` keeps whole datasets, such as
configuration tables, in memory and reloads them only when they change.

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
		store.WithCleanupInterval(time.Second),
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

- `Store[K, V]`: generic key/value storage, TTL, `SetNX`, `Clear`,
  statistics, and coalesced `GetOrLoad`.
- `Bytes[K]`: byte storage with defensive copies plus an explicitly unsafe
  zero-copy read view.
- `Snapshot[T]`: one value loaded as a whole and kept current in the
  background.
- `CompareAndSwap` and `Add`: atomic helpers for comparable and integer values.

Type parameters appear only on constructors such as `New[K, V]`. Options are
plain values; `WithMaxCost` and `WithEvictionCallback` infer their types from
the function you pass, and `New` rejects one whose types do not match.

## Snapshots

A `Snapshot` suits data that is read on every request but changes rarely, such
as a configuration table published to Redis as one large JSON value. `Get`
returns the in-memory value with no I/O; a background poll checks a cheap
version and reloads the value only when the version changes.

```go
gameLib := store.MustNewSnapshot(loadGameLib, // func(context.Context) ([]GameLib, error)
	store.WithName("game_lib"),
	store.WithVersion(loadGameLibMD5), // func(context.Context) (string, error)
	store.WithLoadTimeout(2*time.Second),
)
defer gameLib.Close()

libs, err := gameLib.Get(ctx) // shared value: do not modify it
```

- The first `Get` loads the value; concurrent first calls share one load.
  `Get` returns an error only while no value has been loaded.
- The version is polled every `WithPollInterval` (default 1s). The value is
  also reloaded every `WithMaxAge` (default 5m, zero disables), which is the
  only reload trigger when no version is configured.
- A failed reload or version check keeps the previous value; see
  `SnapshotStats`.
- `Invalidate` forces a background reload, for example when an invalidation
  message arrives.

## Loading

`GetOrLoad` coalesces concurrent misses for a key into one loader call:

- The loader runs on its own goroutine with a context that keeps the first
  caller's values (trace IDs and so on) but not its cancellation. It is
  canceled only after every waiting caller has given up, so one caller's
  timeout does not fail the others.
- A `Set`, `SetNX`, `CompareAndSwap`, `Add`, or `Delete` of the key while a
  load is in flight keeps that load's result out of the store, so an
  invalidation is never overwritten by data read before it. Callers still
  receive the loaded value.
- A loader panic is returned to every waiting caller as `*LoaderPanicError`
  and does not block later loads of the key.
- Values larger than `MaxCost` are returned but not stored.

Options tune loading for an L1 cache in front of Redis or a database:

| Option | Effect |
| --- | --- |
| `WithNegativeTTL(d)` | Stores an `ok=false` result for `d`, so lookups of a missing key stop reaching the loader. Errors are never cached. |
| `WithLoadTimeout(d)` | Bounds each loader call. |
| `WithStaleOnError(grace)` | Keeps expired entries for `grace`; if a reload fails, callers get the old value instead of the error. `Get` never returns expired values. |
| `WithRefreshAhead(window)` | When a hit has less than `window` to live, reloads it in the background and returns the current value immediately. |
| `WithTTLJitter(f)` | Lengthens each TTL by up to `f*TTL` so entries written together do not expire together. |

```go
banned := store.MustNew[int64, struct{}](
	store.WithName("account_ban"),
	store.WithNegativeTTL(30*time.Second), // most users are not banned
	store.WithLoadTimeout(200*time.Millisecond),
	store.WithStaleOnError(time.Minute),
)
```

## Capacity control

Use `WithMaxCost` to bound a store by an application-defined cost. When the
limit is exceeded, the store samples entries across its shards and removes the
least recently accessed candidate.

```go
cache := store.MustNew[string, User](
	store.WithMaxCost(10_000, func(_ string, _ User) int64 {
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
does not expire. TTLs use the monotonic clock, so wall-clock adjustments do not
affect them. Expired values are never returned by `Get`. They are removed
lazily on access and, when `WithCleanupInterval` is configured, by a background
pass; with `WithStaleOnError`, only after the grace period. Call `Close` to
stop that background goroutine.

## Metrics

`Stats` reports hits, misses, cached-absence hits, loads, load errors, stale
fallbacks, background refreshes, evictions, entries, and cost. Stores created
`WithName`, and snapshots created with it, appear in `AllStats()`, and
`PublishExpvar()` exposes them as the expvar variable `go_store`. Names must be
unique among open stores and snapshots; `Close` releases them.

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
