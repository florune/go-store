# go-store

English | [简体中文](README.zh-CN.md)

`go-store` is a typed, in-process L1 cache for Go services that sit in front of
Redis or a database. It keeps hot data in memory, coalesces concurrent loads,
and keeps several processes consistent through Redis pub/sub, so that most
reads never leave the process.

- **`Store[K, V]`**: per-key cache with TTLs, read-through loading, negative
  caching, stale-on-error, refresh-ahead, and optional cost-bounded eviction.
- **`Snapshot[T]`**: a whole dataset, such as a configuration table, held in
  memory and reloaded only when its version changes. Reads are lock-free.
- **`redisbus`**: broadcasts invalidations so that every process drops or
  reloads its copy after a write.

Values are stored as Go values, never serialized. Type parameters appear only
on constructors; options are plain values. Requires Go 1.24 or newer.

```sh
go get github.com/florune/go-store
```

Full API reference: [docs/API.md](docs/API.md) ([中文](docs/API.zh-CN.md)).

## Choosing a type

| Data | Use | Example |
| --- | --- | --- |
| Read on every request, changes rarely, loaded as one value | `Snapshot` | Game library, lobby menus, VIP levels, `config_kv` |
| Per-key, read often, changes occasionally | `Store` + `GetOrLoad` | User profile, ban flag, channel version |
| Must change everywhere at once after a write | Either, bound to `redisbus` | User profile after login or ban |
| Strongly consistent or atomic shared state | **Do not cache in L1** | Wallet balance, locks, Lua counters, idempotency keys |

## Store: read-through cache

```go
users := store.MustNew[int64, User](
	store.WithName("user_info"),
	store.WithMaxCost(100_000, func(int64, User) int64 { return 1 }), // at most 100k users
	store.WithNegativeTTL(30*time.Second),  // cache "not found" too
	store.WithLoadTimeout(2*time.Second),   // bound each load
	store.WithStaleOnError(time.Minute),    // serve the old value if a reload fails
	store.WithTTLJitter(0.1),               // spread expiries
)
defer users.Close()

user, ok, err := users.GetOrLoad(ctx, uid, 5*time.Minute,
	func(ctx context.Context, uid int64) (User, bool, error) {
		u, err := repo.FindUser(ctx, uid) // Redis, then the database
		if errors.Is(err, ErrNotFound) {
			return User{}, false, nil // ok=false: absent, cached for 30s
		}
		return u, err == nil, err // errors are never cached
	})
```

`GetOrLoad` guarantees:

- Concurrent misses for one key share a single loader call.
- The loader runs with the first caller's context values but not its
  cancellation; it is canceled only when every waiting caller has given up.
  One caller timing out never fails the others.
- A `Set`, `SetNX`, `CompareAndSwap`, `Add`, `Delete`, or `Clear` of the key
  while a load is in flight keeps that load's result out of the cache, so an
  invalidation is never overwritten by data read before it.
- A loader panic is returned as `*LoaderPanicError` and never blocks the key.
- With `WithStaleOnError`, a failed reload returns the expired value with a
  **nil error**; the failure shows only in `Stats` (`LoadErrors`,
  `StaleFallbacks`).
- With `WithRefreshAhead(window)`, a hit within `window` of expiry returns at
  once and reloads in the background.

`Store` also offers `Get`, `Set`, `SetNX`, `Delete`, `Expire`, `Clear`, and the
atomic helpers `CompareAndSwap` and `Add`. `SetNX` with a short TTL doubles as
a local write throttle, for example to refresh an online marker at most once
per 30 seconds per user.

## Snapshot: whole datasets

```go
gameLib := store.MustNewSnapshot(loadGameLib, // func(context.Context) ([]GameLib, error)
	store.WithName("game_lib"),
	store.WithVersion(loadGameLibMD5), // func(context.Context) (string, error), polled every second
)
defer gameLib.Close()

libs, err := gameLib.Get(ctx) // in memory; the slice is shared, do not modify it
```

- The first `Get` loads the value, and concurrent first calls share the load.
  `Get` returns an error only while no value has ever been loaded.
- The version is read before the data, so a change made during a load is
  picked up by the next poll.
- The value is also reloaded every `WithMaxAge` (default five minutes), which
  is the only trigger when no version is configured.
- A failed reload or version check keeps the previous value.
- `Invalidate` forces a background reload.

For a 400 KB JSON table read on every request, this replaces one large Redis
read per request with one small version read per second per process.

## Invalidation across processes

```go
bus, _ := redisbus.New(redisClient, "myapp:l1:invalidate") // go-redis v8 UniversalClient
defer bus.Close()

userBinding, _ := redisbus.BindStore(bus, "user_info", users) // string or integer keys
libBinding, _ := redisbus.BindSnapshot(bus, "game_lib", gameLib)

// After the database and the Redis copy are updated:
userBinding.Invalidate(ctx, uid) // deletes locally, then in every other process
libBinding.Invalidate(ctx)       // reloads everywhere
```

A process that writes data but caches nothing, such as an admin service,
creates a `Bus`, binds nothing (so it never subscribes), and calls
`bus.Publish(ctx, "user_info", "42")` or `bus.PublishAll(ctx, "game_lib")`.

- **Invalidate after the shared copy changes.** Invalidating first lets another
  process reload the old value and keep it until its TTL.
- Pub/sub delivers at most once. Whenever the subscription is established or
  re-established, the bus clears every bound store and reloads every bound
  snapshot, so a disconnect cannot leave stale entries behind. Keep TTLs as the
  final bound on staleness.
- Only `PUBLISH` and `SUBSCRIBE` are used, so it works with Redis Cluster and
  ElastiCache Serverless.

## Behavior notes

- **TTL**: a positive TTL sets an expiry; zero or negative means none. TTLs use
  the monotonic clock. Expired entries are never returned by `Get`; they are
  removed on access and by the optional `WithCleanupInterval` pass.
- **Values are shared, not copied.** Treat maps, slices, and pointers stored in
  a `Store` or returned by a `Snapshot` as read-only. `Bytes[K]` copies byte
  slices on every read and write.
- **Capacity**: `WithMaxCost` evicts by sampling a few entries per shard and
  removing the least recently used. It is approximate by design and avoids a
  global lock.
- **Callbacks**: `WithEvictionCallback` runs synchronously after locks are
  released; keep it fast.

## Metrics

`Store.Stats`, `Snapshot.Stats`, and `Bus.Stats` return counters (hits,
misses, cached-absence hits, loads, load errors, stale fallbacks, refreshes,
evictions, resets, and so on). Stores and snapshots created `WithName` are
listed by `store.AllStats()`, and `store.PublishExpvar()` serves them as the
expvar variable `go_store`.

## Performance

Parallel reads on an Intel i5-14400F (16 threads), Go 1.26, zero allocations:

| Operation | Time |
| --- | --- |
| `Store.Get` hit | ~25 ns |
| `Store.GetOrLoad` hit | ~26 ns |
| `Snapshot.Get` | <1 ns |

## Development

```sh
go vet ./...
go test ./...
go test -race ./...
go test -bench=. -benchmem ./...
```

## License

Apache-2.0.
