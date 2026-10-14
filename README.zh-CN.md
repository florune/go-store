# go-store

[English](README.md) | 简体中文

`go-store` 是一个带类型的进程内 L1 缓存，适用于部署在 Redis 或数据库前面的 Go 服务。它把热数据放在内存里，合并并发加载，并通过 Redis Pub/Sub 让多个进程之间保持一致，使绝大多数读请求不出进程。

- **`Store[K, V]`**：按 key 缓存，支持 TTL、回源加载（read-through）、负缓存、出错时返回旧值、提前刷新，以及可选的按 cost 限制容量。
- **`Snapshot[T]`**：把一整份数据集（比如一张配置表）放在内存里，只在版本变化时重新加载，读取无锁。
- **`redisbus`**：广播失效消息，一个进程写入后，所有进程都会删除或重新加载自己的副本。

缓存里存的是 Go 值本身，不做序列化。类型参数只出现在构造函数上，选项都是普通值。需要 Go 1.24 及以上。

```sh
go get github.com/florune/go-store
```

完整 API 文档：[docs/API.zh-CN.md](docs/API.zh-CN.md)（[English](docs/API.md)）。

## 选用哪种类型

| 数据特点 | 用什么 | 例子 |
| --- | --- | --- |
| 每个请求都读、很少变化、作为一个整体加载 | `Snapshot` | 游戏库、大厅菜单、VIP 等级、`config_kv` |
| 按 key 读、读得多、偶尔变化 | `Store` + `GetOrLoad` | 用户信息、封禁标记、渠道版本 |
| 写入后必须所有进程同时生效 | 以上任一种，绑定到 `redisbus` | 登录或封号后的用户信息 |
| 强一致或需要原子操作的共享状态 | **不要放进 L1** | 钱包余额、分布式锁、Lua 计数、幂等 key |

## Store：回源缓存

```go
users := store.MustNew[int64, User](
	store.WithName("user_info"),
	store.WithMaxCost(100_000, func(int64, User) int64 { return 1 }), // 最多 10 万个用户
	store.WithNegativeTTL(30*time.Second),  // "不存在"也缓存
	store.WithLoadTimeout(2*time.Second),   // 每次加载的超时
	store.WithStaleOnError(time.Minute),    // 重新加载失败时返回旧值
	store.WithTTLJitter(0.1),               // 打散过期时间
)
defer users.Close()

user, ok, err := users.GetOrLoad(ctx, uid, 5*time.Minute,
	func(ctx context.Context, uid int64) (User, bool, error) {
		u, err := repo.FindUser(ctx, uid) // 先查 Redis，再查数据库
		if errors.Is(err, ErrNotFound) {
			return User{}, false, nil // ok=false 表示不存在，缓存 30 秒
		}
		return u, err == nil, err // 出错的结果永远不缓存
	})
```

`GetOrLoad` 的保证：

- 同一个 key 的并发未命中只调用一次 loader。
- loader 使用第一个调用方 ctx 里的值，但不跟随它的取消；只有所有等待者都放弃时才取消 loader。一个调用方超时不会连累其他调用方。
- 加载进行期间，如果对这个 key 执行了 `Set`、`SetNX`、`CompareAndSwap`、`Add`、`Delete` 或 `Clear`，这次加载的结果不会写入缓存，失效操作不会被加载前读到的旧数据覆盖。
- loader panic 会以 `*LoaderPanicError` 返回，不会卡住这个 key。
- 开启 `WithStaleOnError` 时，重新加载失败会返回已过期的旧值，并且 **err 为 nil**；失败只能从 `Stats` 里看到（`LoadErrors`、`StaleFallbacks`）。
- 开启 `WithRefreshAhead(window)` 时，命中一个距离过期不到 `window` 的值会立即返回，同时在后台重新加载。

`Store` 还提供 `Get`、`Set`、`SetNX`、`Delete`、`Expire`、`Clear`，以及原子操作 `CompareAndSwap` 和 `Add`。`SetNX` 配合短 TTL 可以当作本地写入限流器，比如每个用户每 30 秒最多刷新一次在线标记。

## Snapshot：整份数据集

```go
gameLib := store.MustNewSnapshot(loadGameLib, // func(context.Context) ([]GameLib, error)
	store.WithName("game_lib"),
	store.WithVersion(loadGameLibMD5), // func(context.Context) (string, error)，每秒检查一次
)
defer gameLib.Close()

libs, err := gameLib.Get(ctx) // 读内存；返回的切片是共享的，不要修改
```

- 首次 `Get` 时加载，并发的首次调用共享同一次加载。只有在一次都没加载成功过的时候，`Get` 才会返回错误。
- 先读版本号再读数据，所以加载期间发生的修改会在下一次轮询时被发现。
- 每隔 `WithMaxAge`（默认 5 分钟）也会重新加载一次；没有配置版本号时，这是唯一的刷新方式。
- 重新加载或版本检查失败时，继续使用旧值。
- `Invalidate` 会在后台强制重新加载。

以一张每个请求都要读的 400KB JSON 配置表为例：原来每个请求读一次大 value，现在每个进程每秒只读一次很小的版本号。

## 跨进程失效

```go
bus, _ := redisbus.New(redisClient, "myapp:l1:invalidate") // go-redis v8 的 UniversalClient
defer bus.Close()

userBinding, _ := redisbus.BindStore(bus, "user_info", users) // key 为字符串或整数类型
libBinding, _ := redisbus.BindSnapshot(bus, "game_lib", gameLib)

// 在数据库和 Redis 都更新之后：
userBinding.Invalidate(ctx, uid) // 先删本进程，再删其他所有进程
libBinding.Invalidate(ctx)       // 所有进程重新加载
```

只写数据、不持有缓存的进程（比如 admin 后台）创建一个 `Bus`，不做任何绑定（因此不会订阅），直接调用 `bus.Publish(ctx, "user_info", "42")` 或 `bus.PublishAll(ctx, "game_lib")`。

- **一定要在共享数据更新之后再失效。** 顺序反了，其他进程可能重新加载到旧值，并一直保留到 TTL 过期。
- Pub/Sub 至多投递一次。每当订阅建立或重新建立时，Bus 会清空所有绑定的 Store、重新加载所有绑定的 Snapshot，所以断线期间丢失的消息不会留下脏数据。TTL 仍然是数据过时时间的最终上限。
- 只用到 `PUBLISH` 和 `SUBSCRIBE`，兼容 Redis Cluster 和 ElastiCache Serverless。

## 行为说明

- **TTL**：大于 0 表示有过期时间；0 或负数表示不过期。TTL 使用单调时钟。`Get` 永远不会返回已过期的条目；过期条目在访问时被删除，也可以通过 `WithCleanupInterval` 定期清理。
- **值是共享的，不是拷贝。** 存进 `Store` 或由 `Snapshot` 返回的 map、切片、指针都应当只读。`Bytes[K]` 在每次读写时都会拷贝字节切片。
- **容量**：`WithMaxCost` 通过在每个分片里抽样几个条目、淘汰最久未访问的那个来控制容量。这是有意设计成近似的，以避免全局锁。
- **回调**：`WithEvictionCallback` 在释放锁之后同步执行，应尽快返回。

## 监控指标

`Store.Stats`、`Snapshot.Stats`、`Bus.Stats` 返回各项计数（命中、未命中、负缓存命中、加载、加载错误、旧值兜底、提前刷新、淘汰、订阅重置等）。用 `WithName` 创建的 Store 和 Snapshot 会出现在 `store.AllStats()` 里，`store.PublishExpvar()` 会把它们发布为 expvar 变量 `go_store`。

## 性能

Intel i5-14400F（16 线程）、Go 1.26 上的并发读取，均为 0 分配：

| 操作 | 耗时 |
| --- | --- |
| `Store.Get` 命中 | 约 25 ns |
| `Store.GetOrLoad` 命中 | 约 26 ns |
| `Snapshot.Get` | 小于 1 ns |

## 开发

```sh
go vet ./...
go test ./...
go test -race ./...
go test -bench=. -benchmem ./...
```

## 许可证

Apache-2.0。
