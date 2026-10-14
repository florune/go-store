# API 参考

[English](API.md) | 简体中文

包 `store`（`github.com/florune/go-store`）和子包 `redisbus`（`github.com/florune/go-store/redisbus`）。所有类型都可以并发使用。除非另有说明，时长参数为 0 表示"关闭"。

- [Store](#store)
- [Store 选项](#store-选项)
- [Loader 与错误](#loader-与错误)
- [原子操作](#原子操作)
- [Bytes](#bytes)
- [Snapshot](#snapshot)
- [Snapshot 选项](#snapshot-选项)
- [共享选项](#共享选项)
- [监控指标](#监控指标)
- [redisbus](#redisbus)

---

## Store

```go
type Store[K comparable, V any] struct{ /* 未导出字段 */ }
```

分片的、带类型的 key/value 缓存。缓存里存的是 Go 值本身；值内部的 map、切片、指针会和调用方共享，应当只读。

### New、MustNew

```go
func New[K comparable, V any](options ...Option) (*Store[K, V], error)
func MustNew[K comparable, V any](options ...Option) *Store[K, V]
```

创建一个 Store。以下情况 `New` 返回错误：选项不合法；传给 `WithMaxCost` 或 `WithEvictionCallback` 的函数类型与 `K`、`V` 不匹配；`WithName` 指定的名字已被某个未关闭的 Store 或 Snapshot 使用。`MustNew` 遇到这些情况会 panic。

```go
users := store.MustNew[int64, User](store.WithNegativeTTL(30 * time.Second))
```

### GetOrLoad

```go
func (s *Store[K, V]) GetOrLoad(ctx context.Context, key K, ttl time.Duration, loader Loader[K, V]) (V, bool, error)
```

返回 `key` 的缓存值；没有缓存时调用 `loader`，并把结果缓存 `ttl`。

| 情况 | 返回值 |
| --- | --- |
| 有未过期的缓存值 | `value, true, nil` |
| 命中负缓存（`WithNegativeTTL`） | `zero, false, nil`，不调用 loader |
| loader 返回 `v, true, nil` | `v, true, nil`，缓存 `ttl` |
| loader 返回 `_, false, nil` | `zero, false, nil`；只有开启 `WithNegativeTTL` 时才缓存为"不存在" |
| loader 返回错误 | `_, _, err`，永远不缓存 |
| loader 失败，且过期旧值仍在 `WithStaleOnError` 宽限期内 | `old, true, nil`（错误被隐藏，见 `Stats.StaleFallbacks`） |
| loader panic | `*LoaderPanicError` |
| `ctx` 先结束 | `zero, false, ctx.Err()`；加载会继续，供其他调用方使用 |

行为：

- 同一个缺失 key 的并发调用只触发一次 loader。
- loader 在独立的 goroutine 里运行。它的 ctx 保留第一个调用方 ctx 里的值，但不跟随其取消；受 `WithLoadTimeout` 限制；所有等待者都离开后会被取消。
- 加载期间如果 `Set`、`SetNX`、`CompareAndSwap`、`Add`、`Delete` 或 `Clear` 操作了这个 key，加载结果照常返回，但不写入缓存。
- cost 超过 `WithMaxCost` 的值照常返回，但不写入缓存。
- 开启 `WithRefreshAhead` 时，命中一个快要过期的值还会在后台触发重新加载。

### Get

```go
func (s *Store[K, V]) Get(key K) (V, bool)
```

返回未过期的值，并记录访问（用于淘汰）。key 不存在、已过期或是负缓存时返回 `zero, false`。不会触发加载。

### Set

```go
func (s *Store[K, V]) Set(key K, value V, ttl time.Duration) bool
```

把 `value` 存 `ttl`（0 或负数表示不过期），覆盖已有条目。只有当这个值自身的 cost 就超过 `WithMaxCost` 时才返回 `false`，此时缓存保持不变。覆盖不会触发淘汰回调。

### SetNX

```go
func (s *Store[K, V]) SetNX(key K, value V, ttl time.Duration) bool
```

只有在 `key` 没有未过期的值时才写入；负缓存不算作有值。返回是否写入。可以用作本地限流：`if s.SetNX(uid, struct{}{}, 30*time.Second) { /* 执行写操作 */ }`。

### Delete

```go
func (s *Store[K, V]) Delete(key K) bool
```

删除 `key`，返回是否存在过条目（包括未过期、已过期和负缓存条目）。该 key 正在进行的加载不会把结果写入缓存。不会触发淘汰回调。

### Expire

```go
func (s *Store[K, V]) Expire(key K, ttl time.Duration) bool
```

给一个未过期的值设置新的 TTL；0 或负数表示去掉过期时间。key 不存在、是负缓存或已过期时返回 `false`（已过期的条目会被顺便删除）。

### Clear

```go
func (s *Store[K, V]) Clear()
```

删除所有条目，并让所有正在进行的加载都不写入结果。不会触发淘汰回调。

### Len、Stats、Name、Close

```go
func (s *Store[K, V]) Len() int
func (s *Store[K, V]) Stats() Stats
func (s *Store[K, V]) Name() string
func (s *Store[K, V]) Close()
```

- `Len`：条目数量，包括负缓存和尚未清理的过期条目。
- `Stats`：各项计数，见[监控指标](#监控指标)。
- `Name`：`WithName` 设置的名字，没有设置时为 `""`。
- `Close`：停止后台清理 goroutine 并释放名字。之后 Store 仍可正常使用。重复调用是安全的。

---

## Store 选项

都返回 `Option`，传给 `New` / `MustNew`。

| 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `WithMaxCost(max, cost)` | 不限 | 限制所有条目 `cost(key, value)` 的总和，超出时近似按最久未访问淘汰。单个值的 cost 超过 `max` 会被拒绝。`cost` 必须是 `func(K, V) int64`；负数 cost 按 0 计。 |
| `WithNegativeTTL(d)` | 关闭 | 把 loader 返回 `ok=false` 的结果缓存 `d`。出错的结果永远不缓存。 |
| `WithStaleOnError(grace)` | 关闭 | 过期条目额外保留 `grace`。`Get` 不会返回它们，但 `GetOrLoad` 重新加载失败时会返回它们，并且 err 为 nil。 |
| `WithRefreshAhead(window)` | 关闭 | `GetOrLoad` 命中一个剩余时间不足 `window` 的值时立即返回，并在后台重新加载（每个条目只刷新一次；失败后下次命中时重试）。 |
| `WithTTLJitter(f)` | 0 | 给每个正 TTL 随机增加 0 到 `f*TTL`，`0 ≤ f ≤ 1`。对所有写入和 `Expire` 都生效。 |
| `WithCleanupInterval(d)` | 关闭 | 每隔 `d` 在后台清理过期条目。不开启时，过期条目在被访问时删除。 |
| `WithEvictionCallback(fn)` | 无 | 条目过期（`Expired`）或因容量被淘汰（`Capacity`）时同步调用 `fn(key, value, reason)`。`Delete`、`Clear`、覆盖写入和负缓存条目不会触发。`fn` 必须是 `func(K, V, EvictionReason)`。 |
| `WithShards(n)` | 64 | 独立加锁的分片数量，必须是 2 的幂。 |

```go
type EvictionReason uint8
const (
	Expired  EvictionReason = iota + 1 // TTL 到期
	Capacity                           // 被 WithMaxCost 淘汰
)
```

---

## Loader 与错误

```go
type Loader[K comparable, V any] func(ctx context.Context, key K) (value V, ok bool, err error)
```

获取缺失的值。值不存在时（比如 Redis 返回 `redis.Nil`、数据库里也没有这一行）返回 `ok=false` 且 err 为 nil；只有真正出错时才返回 error。

```go
type LoaderPanicError struct {
	Value any    // panic 的参数
	Stack []byte // loader goroutine 的调用栈
}
func (e *LoaderPanicError) Error() string
func (e *LoaderPanicError) Unwrap() error // Value 是 error 时返回它
```

loader panic 时，`GetOrLoad` 和 `Snapshot.Get` 返回这个错误。

---

## 原子操作

```go
func CompareAndSwap[K comparable, V comparable](s *Store[K, V], key K, old, next V, ttl time.Duration) bool
func Add[K comparable, V Integer](s *Store[K, V], key K, delta V, ttl time.Duration) V
type Integer interface{ ~int | ~int8 | ... | ~uint64 | ~uintptr }
```

- `CompareAndSwap`：当未过期的值等于 `old` 时，替换为 `next` 并设置新 TTL。key 不存在、已过期、是负缓存、值不相等，或 `next` 超过 `WithMaxCost` 时返回 `false`。
- `Add`：加上 `delta` 并返回新值；key 不存在、已过期或是负缓存时从 0 开始。每次调用都会重设 TTL，不受 `WithMaxCost` 拒绝（但可能触发淘汰）。

两者都只在本进程内生效。需要跨进程共享的计数请使用 Redis。

---

## Bytes

```go
type Bytes[K comparable] struct{ /* 未导出字段 */ }
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

一个在 `Set`、`SetNX`、`Get` 时都会拷贝数据的 `Store[K, []byte]`，调用方可以放心复用或修改自己的切片。`GetUnsafeView` 不做拷贝：不要修改返回值，也不要长期持有。

---

## Snapshot

```go
type Snapshot[T any] struct{ /* 未导出字段 */ }
```

作为整体加载、并在后台保持最新的一个值。`Get` 只读一个原子指针：没有锁、没有拷贝、没有 I/O。返回的值是共享的，应当只读。

### NewSnapshot、MustNewSnapshot

```go
func NewSnapshot[T any](load func(context.Context) (T, error), options ...SnapshotOption) (*Snapshot[T], error)
func MustNewSnapshot[T any](load func(context.Context) (T, error), options ...SnapshotOption) *Snapshot[T]
```

创建一个用 `load` 加载数据的 Snapshot。首次 `Get` 之前不会加载。`load` 为 nil、选项不合法或名字已被占用时返回错误。

### Get

```go
func (s *Snapshot[T]) Get(ctx context.Context) (T, error)
```

返回当前值。首次使用时加载，并发的首次调用共享这次加载。只有在一次都没加载成功过时才返回错误：加载的错误，或者 `ctx` 先结束时返回 `ctx.Err()`（加载会在后台继续）。首次成功之后，总是返回最近一次加载的值，err 为 nil。

### Invalidate

```go
func (s *Snapshot[T]) Invalidate()
```

不管版本号是否变化，都在后台重新加载。重新加载成功之前，`Get` 继续返回旧值。如果已经有一次加载在进行，结束后会再加载一次，因为正在进行的那次可能读到了旧数据。如果还没加载过、也没有加载在进行，什么都不做。

### Stats、Close

```go
func (s *Snapshot[T]) Stats() SnapshotStats
func (s *Snapshot[T]) Close()
```

`Close` 停止后台轮询并释放名字；之后 `Get` 继续返回最后一次的值。重复调用是安全的。

### 重新加载规则

已经有值之后，每次轮询：

1. 如果距离上次加载已超过 `WithMaxAge`，或者没有配置版本号，就重新加载。
2. 否则读取版本号，只有和已加载的版本不同时才重新加载。

版本号在数据之前读取。加载或版本检查失败时继续使用旧值，并计入 `SnapshotStats.LoadErrors`。

---

## Snapshot 选项

都返回 `SnapshotOption`，传给 `NewSnapshot` / `MustNewSnapshot`。

| 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `WithVersion(fn)` | 无 | `fn(ctx) (string, error)` 返回一个读取成本很低的版本号，比如和数据放在一起的哈希值。只有版本号变化时才重新加载。 |
| `WithPollInterval(d)` | 1 秒 | 读取版本号的间隔，必须大于 0。 |
| `WithMaxAge(d)` | 5 分钟 | 即使版本号没变，也至少每隔这么久重新加载一次；没有配置版本号时这是唯一的刷新方式。0 表示关闭。 |

---

## 共享选项

返回 `SharedOption`，`New` 和 `NewSnapshot` 都可以使用。

| 选项 | 作用 |
| --- | --- |
| `WithName(name)` | 让 Store 或 Snapshot 出现在 `AllStats` 和 `PublishExpvar` 里。不能为空，且在所有未关闭的 Store 和 Snapshot 中唯一；`Close` 时释放。 |
| `WithLoadTimeout(d)` | 限制每次 `GetOrLoad` loader 调用的时长，或每次 Snapshot 加载和版本检查的时长。 |

---

## 监控指标

```go
type Stats struct {
	Hits, Misses, Evictions, Loads, LoadErrors uint64
	NegativeHits   uint64 // 由负缓存回答的查询
	StaleFallbacks uint64 // 加载失败后返回旧值的次数
	Refreshes      uint64 // WithRefreshAhead 启动的后台刷新
	Entries        int
	Cost           int64
}

type SnapshotStats struct {
	Loads, LoadErrors, VersionChecks uint64
	Loaded   bool      // 是否已加载过值
	Version  string    // 当前值的版本号
	LoadedAt time.Time // 当前值的加载时间
}

func AllStats() map[string]any // 名字 -> Stats 或 SnapshotStats，仅包含已命名且未关闭的 Store 和 Snapshot
func PublishExpvar()           // 把 AllStats 发布为 expvar 变量 "go_store"；可重复调用
```

`Evictions` 同时包含过期和容量淘汰。`Misses` 包含 `Get` 未命中，以及需要等待加载的 `GetOrLoad` 调用。

---

## redisbus

```go
import "github.com/florune/go-store/redisbus"
```

通过 Redis Pub/Sub（go-redis v8）广播失效消息，只用到 `PUBLISH` 和 `SUBSCRIBE`。

### New

```go
func New(client redis.UniversalClient, channel string) (*Bus, error)
```

在 `channel` 上创建一个 Bus。需要共享失效消息的所有进程使用同一个频道。第一次绑定时才订阅；只发布的 Bus 永远不会订阅。

### Bus 的方法

```go
func (b *Bus) Publish(ctx context.Context, namespace string, keys ...string) error
func (b *Bus) PublishAll(ctx context.Context, namespace string) error
func (b *Bus) Stats() Stats
func (b *Bus) Close() error
```

- `Publish`：通知其他进程失效 `namespace` 下的 `keys`。key 的字符串形式与 `BindStore` 的编码一致：字符串本身，或者整数的十进制形式。没有 key 时不发送消息。
- `PublishAll`：通知其他进程清空 `namespace` 绑定的 Store，或重新加载绑定的 Snapshot。
- `Close`：停止接收消息。绑定的缓存内容保持不变。

进程会忽略自己发出的消息。

### BindStore

```go
func BindStore[K comparable, V any](b *Bus, namespace string, s *store.Store[K, V]) (*StoreBinding[K], error)
func (sb *StoreBinding[K]) Invalidate(ctx context.Context, keys ...K) error
func (sb *StoreBinding[K]) InvalidateAll(ctx context.Context) error
```

把 Store 绑定到 `namespace`。`K` 的底层类型必须是字符串或整数（`type UID int64` 这类自定义类型也可以）。key 是其他类型、namespace 为空或已被绑定、Bus 已关闭时返回错误。

- `Invalidate`：先删除本进程的 `keys`，再发布。发布失败时本地删除仍然有效，并返回错误。
- `InvalidateAll`：先清空本进程的 Store，再发布。

### BindSnapshot

```go
func BindSnapshot[T any](b *Bus, namespace string, s *store.Snapshot[T]) (*SnapshotBinding, error)
func (sb *SnapshotBinding) Invalidate(ctx context.Context) error
```

把 Snapshot 绑定到 `namespace`。`Invalidate` 先重新加载本进程的 Snapshot，再通知其他进程重新加载。

### Stats

```go
type Stats struct {
	Published, Received uint64 // 发送的消息 / 处理的消息
	Resets              uint64 // 订阅建立或重新建立的次数，每次都会清空所有绑定的缓存
	Dropped             uint64 // 格式错误、未知 namespace 或 key 无法解析的消息
}
```

### 顺序与投递

1. 先更新数据库和 Redis 里的副本。
2. 然后调用 `Invalidate`（或 `Publish`）。

顺序反过来，其他进程可能重新加载到旧值。订阅方断线期间发送的消息会被 Pub/Sub 丢弃，所以每次订阅建立或重新建立时，Bus 都会清空所有绑定的 Store、重新加载所有绑定的 Snapshot。TTL 仍然是最终兜底。

### 消息格式

每条消息是频道上的一段 JSON，其他语言也可以发布：

```json
{"from": "<发送方的随机 id>", "ns": "user_info", "keys": ["42", "43"]}
{"from": "<发送方的随机 id>", "ns": "game_lib", "all": true}
```

进程只会跳过 `from` 等于自己随机 id 的消息，所以填任何其他值（比如 `"admin-php"`）都可以。
