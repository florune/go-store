# API reference

English | [简体中文](API.zh-CN.md)

Package `store` (`github.com/florune/go-store`) and subpackage `redisbus`
(`github.com/florune/go-store/redisbus`). All types are safe for concurrent
use. Durations of zero mean "disabled" unless stated otherwise.

- [Store](#store)
- [Store options](#store-options)
- [Loader and errors](#loader-and-errors)
- [Atomic helpers](#atomic-helpers)
- [Bytes](#bytes)
- [Snapshot](#snapshot)
- [Snapshot options](#snapshot-options)
- [Shared options](#shared-options)
- [Metrics](#metrics)
- [redisbus](#redisbus)

---

## Store

```go
type Store[K comparable, V any] struct{ /* unexported */ }
```

A sharded, typed key/value cache. Values are stored as Go values; maps,
slices, and pointers inside them are shared with callers, so treat them as
read-only.

### New, MustNew

```go
func New[K comparable, V any](options ...Option) (*Store[K, V], error)
func MustNew[K comparable, V any](options ...Option) *Store[K, V]
```

Creates a store. `New` returns an error when an option is invalid, when the
function given to `WithMaxCost` or `WithEvictionCallback` does not match
`K` and `V`, or when the `WithName` name is already used by an open store or
snapshot. `MustNew` panics instead.

```go
users := store.MustNew[int64, User](store.WithNegativeTTL(30 * time.Second))
```

### GetOrLoad

```go
func (s *Store[K, V]) GetOrLoad(ctx context.Context, key K, ttl time.Duration, loader Loader[K, V]) (V, bool, error)
```

Returns the cached value for `key`, or calls `loader` and caches its result for
`ttl`.

| Situation | Returns |
| --- | --- |
| Live value cached | `value, true, nil` |
| Cached absence (`WithNegativeTTL`) | `zero, false, nil` without calling the loader |
| Loader returns `v, true, nil` | `v, true, nil`; cached for `ttl` |
| Loader returns `_, false, nil` | `zero, false, nil`; cached as absent only with `WithNegativeTTL` |
| Loader returns an error | `_, _, err`; never cached |
| Loader fails, expired value within `WithStaleOnError` grace | `old, true, nil` (the error is hidden; see `Stats.StaleFallbacks`) |
| Loader panics | `*LoaderPanicError` |
| `ctx` ends first | `zero, false, ctx.Err()`; the load continues for other callers |

Behavior:

- Concurrent calls for the same missing key share one loader call.
- The loader runs on its own goroutine. Its context keeps the first caller's
  values but not its cancellation, is bounded by `WithLoadTimeout`, and is
  canceled once every waiting caller has left.
- If `Set`, `SetNX`, `CompareAndSwap`, `Add`, `Delete`, or `Clear` touches the
  key while the load runs, the result is returned but not cached.
- A value whose cost exceeds `WithMaxCost` is returned but not cached.
- With `WithRefreshAhead`, a hit close to expiry also starts a background
  reload.

### Get

```go
func (s *Store[K, V]) Get(key K) (V, bool)
```

Returns the live value and records the access for eviction. A missing,
expired, or absent (negatively cached) key returns `zero, false`. Never loads.

### Set

```go
func (s *Store[K, V]) Set(key K, value V, ttl time.Duration) bool
```

Stores `value` for `ttl` (zero or negative: no expiry), replacing any existing
entry. Returns `false`, leaving the store unchanged, only when the value's
cost alone exceeds `WithMaxCost`. Replacement does not call the eviction
callback.

### SetNX

```go
func (s *Store[K, V]) SetNX(key K, value V, ttl time.Duration) bool
```

Stores `value` only if `key` has no live value; a cached absence does not
count as a value. Returns whether it stored. Useful as a local throttle:
`if s.SetNX(uid, struct{}{}, 30*time.Second) { /* do the write */ }`.

### Delete

```go
func (s *Store[K, V]) Delete(key K) bool
```

Removes `key` and reports whether an entry (live, expired, or absent) existed.
A load of `key` in flight will not cache its result. Does not call the
eviction callback.

### Expire

```go
func (s *Store[K, V]) Expire(key K, ttl time.Duration) bool
```

Sets a new TTL on a live value; zero or negative removes the expiry. Returns
`false` for a missing key, a cached absence, or an expired entry (which it
removes).

### Clear

```go
func (s *Store[K, V]) Clear()
```

Removes every entry and keeps every load in flight from caching its result.
Does not call the eviction callback.

### Len, Stats, Name, Close

```go
func (s *Store[K, V]) Len() int
func (s *Store[K, V]) Stats() Stats
func (s *Store[K, V]) Name() string
func (s *Store[K, V]) Close()
```

- `Len`: number of entries, including cached absences and expired entries not
  yet removed.
- `Stats`: counters; see [Metrics](#metrics).
- `Name`: the `WithName` name, or `""`.
- `Close`: stops the cleanup goroutine and releases the name. The store keeps
  working. Safe to call more than once.

---

## Store options

All return `Option` and are passed to `New` / `MustNew`.

| Option | Default | Effect |
| --- | --- | --- |
| `WithMaxCost(max, cost)` | unbounded | Bounds the sum of `cost(key, value)`. When exceeded, evicts approximately least-recently-used entries. A single value costing more than `max` is rejected. `cost` must be `func(K, V) int64`; negative costs count as zero. |
| `WithNegativeTTL(d)` | off | Caches `ok=false` loader results for `d`. Errors are never cached. |
| `WithStaleOnError(grace)` | off | Keeps expired entries for `grace`. `Get` ignores them, but a failed `GetOrLoad` reload returns them with a nil error. |
| `WithRefreshAhead(window)` | off | A `GetOrLoad` hit with less than `window` to live returns immediately and reloads in the background (one reload per entry; retried on a later hit if it fails). |
| `WithTTLJitter(f)` | 0 | Lengthens each positive TTL by a random 0 to `f*TTL`, `0 ≤ f ≤ 1`. Applies to every write and to `Expire`. |
| `WithCleanupInterval(d)` | off | Removes expired entries every `d` in the background. Without it, expired entries are removed when accessed. |
| `WithEvictionCallback(fn)` | none | Calls `fn(key, value, reason)` synchronously when an entry expires (`Expired`) or is evicted for capacity (`Capacity`). Not called for `Delete`, `Clear`, replacement, or cached absences. `fn` must be `func(K, V, EvictionReason)`. |
| `WithShards(n)` | 64 | Number of independently locked shards; a power of two. |

```go
type EvictionReason uint8
const (
	Expired  EvictionReason = iota + 1 // TTL elapsed
	Capacity                           // evicted by WithMaxCost
)
```

---

## Loader and errors

```go
type Loader[K comparable, V any] func(ctx context.Context, key K) (value V, ok bool, err error)
```

Fetches a missing value. Return `ok=false` with a nil error when the value
does not exist (for example `redis.Nil` and then no database row); return an
error only for failures.

```go
type LoaderPanicError struct {
	Value any    // the value passed to panic
	Stack []byte // the loader goroutine's stack
}
func (e *LoaderPanicError) Error() string
func (e *LoaderPanicError) Unwrap() error // Value, when it is an error
```

Returned by `GetOrLoad` and `Snapshot.Get` when a loader panics.

---

## Atomic helpers

```go
func CompareAndSwap[K comparable, V comparable](s *Store[K, V], key K, old, next V, ttl time.Duration) bool
func Add[K comparable, V Integer](s *Store[K, V], key K, delta V, ttl time.Duration) V
type Integer interface{ ~int | ~int8 | ... | ~uint64 | ~uintptr }
```

- `CompareAndSwap`: replaces a live value equal to `old` with `next` and a new
  TTL. Returns `false` if the key is missing, expired, absent, different, or
  if `next` exceeds `WithMaxCost`.
- `Add`: adds `delta` and returns the new value, starting from zero for a
  missing, expired, or absent key. It sets the TTL on every call and is not
  rejected by `WithMaxCost` (it may trigger eviction).

Both are local to the process. For counters shared across processes, use
Redis.

---

## Bytes

```go
type Bytes[K comparable] struct{ /* unexported */ }
func NewBytes[K comparable](options ...Option) (*Bytes[K], error)
func (b *Bytes[K]) Set(key K, value []byte, ttl time.Duration) bool
func (b *Bytes[K]) SetNX(key K, value []byte, ttl time.Duration) bool
func (b *Bytes[K]) Get(key K) ([]byte, bool)
func (b *Bytes[K]) GetUnsafeView(key K) ([]byte, bool)
func (b *Bytes[K]) Delete(key K) bool
func (b *Bytes[K]) Expire(key K, ttl time.Duration) bool
func (b *Bytes[K]) Stats() Stats
func (b *Bytes[K]) Close()
```

A `Store[K, []byte]` that copies on `Set`, `SetNX`, and `Get`, so callers can
reuse or modify their slices. `GetUnsafeView` skips the copy: never modify the
result and do not keep it.

---

## Snapshot

```go
type Snapshot[T any] struct{ /* unexported */ }
```

One value loaded as a whole and kept current in the background. `Get` reads
an atomic pointer: no lock, copy, or I/O. The value is shared; treat it as
read-only.

### NewSnapshot, MustNewSnapshot

```go
func NewSnapshot[T any](load func(context.Context) (T, error), options ...SnapshotOption) (*Snapshot[T], error)
func MustNewSnapshot[T any](load func(context.Context) (T, error), options ...SnapshotOption) *Snapshot[T]
```

Creates a snapshot that loads with `load`. Nothing is loaded until the first
`Get`. Returns an error for a nil `load`, an invalid option, or a name already
in use.

### Get

```go
func (s *Snapshot[T]) Get(ctx context.Context) (T, error)
```

Returns the current value. On first use it loads the value, and concurrent
first calls share that load. It returns an error only while no value has ever
loaded: the load's error, or `ctx.Err()` if `ctx` ends first (the load
continues in the background). After the first success it always returns the
latest loaded value and a nil error.

### Invalidate

```go
func (s *Snapshot[T]) Invalidate()
```

Reloads in the background regardless of the version. `Get` returns the old
value until the reload succeeds. If a load is already running, another one
follows it, since the running one may have read old data. If nothing has been
loaded and no load is running, it does nothing.

### Stats, Close

```go
func (s *Snapshot[T]) Stats() SnapshotStats
func (s *Snapshot[T]) Close()
```

`Close` stops background polling and releases the name; `Get` keeps returning
the last value. Safe to call more than once.

### Reload rules

On each poll tick, once a value exists:

1. If `WithMaxAge` has elapsed since the last load, or no version is
   configured, reload.
2. Otherwise read the version; reload only if it differs from the loaded one.

The version is read before the data. A failed load or version check keeps the
previous value and counts in `SnapshotStats.LoadErrors`.

---

## Snapshot options

All return `SnapshotOption` and are passed to `NewSnapshot` / `MustNewSnapshot`.

| Option | Default | Effect |
| --- | --- | --- |
| `WithVersion(fn)` | none | `fn(ctx) (string, error)` returns a cheap version, such as a hash stored next to the data. The value reloads only when it changes. |
| `WithPollInterval(d)` | 1s | How often the version is read. Must be positive. |
| `WithMaxAge(d)` | 5m | Reload at least this often even if the version is unchanged; the only trigger when no version is set. Zero disables. |

---

## Shared options

Return `SharedOption`, accepted by both `New` and `NewSnapshot`.

| Option | Effect |
| --- | --- |
| `WithName(name)` | Lists the store or snapshot in `AllStats` and `PublishExpvar`. Must be non-empty and unique among open stores and snapshots; `Close` releases it. |
| `WithLoadTimeout(d)` | Bounds each `GetOrLoad` loader call, or each snapshot load and version check. |

---

## Metrics

```go
type Stats struct {
	Hits, Misses, Evictions, Loads, LoadErrors uint64
	NegativeHits   uint64 // lookups answered by a cached absence
	StaleFallbacks uint64 // failed loads that returned the previous value
	Refreshes      uint64 // background reloads started by WithRefreshAhead
	Entries        int
	Cost           int64
}

type SnapshotStats struct {
	Loads, LoadErrors, VersionChecks uint64
	Loaded   bool      // a value has been loaded
	Version  string    // version of the loaded value
	LoadedAt time.Time // when it was loaded
}

func AllStats() map[string]any // name -> Stats or SnapshotStats, for named open stores and snapshots
func PublishExpvar()           // publishes AllStats as expvar "go_store"; safe to call repeatedly
```

`Evictions` counts both expirations and capacity evictions. `Misses` counts
`Get` misses and `GetOrLoad` calls that had to wait for a load.

---

## redisbus

```go
import "github.com/florune/go-store/redisbus"
```

Broadcasts invalidations over Redis pub/sub (go-redis v8). Uses only
`PUBLISH` and `SUBSCRIBE`.

### New

```go
func New(client redis.UniversalClient, channel string) (*Bus, error)
```

Creates a bus on `channel`. Every process that should share invalidations uses
the same channel. The bus subscribes on the first bind; a bus that only
publishes never subscribes.

### Bus methods

```go
func (b *Bus) Publish(ctx context.Context, namespace string, keys ...string) error
func (b *Bus) PublishAll(ctx context.Context, namespace string) error
func (b *Bus) Stats() Stats
func (b *Bus) Close() error
```

- `Publish`: tells other processes to invalidate `keys` in `namespace`. Keys are
  strings as encoded by `BindStore`: the string itself, or an integer in base
  10. No keys: no message.
- `PublishAll`: tells other processes to clear the store, or reload the
  snapshot, bound to `namespace`.
- `Close`: stops receiving. Bound caches keep their contents.

A process ignores its own messages.

### BindStore

```go
func BindStore[K comparable, V any](b *Bus, namespace string, s *store.Store[K, V]) (*StoreBinding[K], error)
func (sb *StoreBinding[K]) Invalidate(ctx context.Context, keys ...K) error
func (sb *StoreBinding[K]) InvalidateAll(ctx context.Context) error
```

Binds a store under `namespace`. `K` must have a string or integer underlying
type (named types such as `type UID int64` work). Returns an error for any
other key type, an empty or already bound namespace, or a closed bus.

- `Invalidate`: deletes `keys` locally, then publishes them. If publishing
  fails, the local deletion stands and the error is returned.
- `InvalidateAll`: clears the local store, then publishes.

### BindSnapshot

```go
func BindSnapshot[T any](b *Bus, namespace string, s *store.Snapshot[T]) (*SnapshotBinding, error)
func (sb *SnapshotBinding) Invalidate(ctx context.Context) error
```

Binds a snapshot under `namespace`. `Invalidate` reloads the local snapshot,
then tells other processes to reload theirs.

### Stats

```go
type Stats struct {
	Published, Received uint64 // messages sent / acted on
	Resets              uint64 // subscriptions (re)established, each clearing every bound cache
	Dropped             uint64 // malformed, unknown namespace, or undecodable keys
}
```

### Ordering and delivery

1. Update the database and the Redis copy.
2. Then call `Invalidate` (or `Publish`).

Reversing the order lets another process reload the old value. Pub/sub drops
messages sent while a subscriber is disconnected, so each time the
subscription is established or re-established, the bus clears every bound
store and reloads every bound snapshot. TTLs remain the final bound.

### Message format

Each message is JSON on the channel, so other languages can publish too:

```json
{"from": "<random id of the sender>", "ns": "user_info", "keys": ["42", "43"]}
{"from": "<random id of the sender>", "ns": "game_lib", "all": true}
```

A process skips messages whose `from` equals its own random id, so any other
value (for example `"admin-php"`) works.
