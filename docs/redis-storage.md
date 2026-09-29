# Redis 幂等记录（P3.3）

RedisStore 提供面向单个业务命令的 TTL 幂等状态机。调用方应为每个命令使用独立 key，并把 operationId 或业务请求 ID 放入 token；不要把普通计数器、租约 key 与幂等 key 混用。

状态只有三种可见结果：

- claimIdempotency(key, token, pendingTtl) 在 key 不存在时原子写入 P:<token>，返回 ACQUIRED；相同 token 的重放会刷新 pending TTL 并返回 ACQUIRED；其他待处理 token 返回 IN_PROGRESS；已有完成结果返回 COMPLETED 和原始 result。
- completeIdempotency(key, token, result, resultTtl) 只有当前值仍是本 token 时才原子转换为完成状态，返回 APPLIED。重复提交相同结果返回 ALREADY_COMPLETED；不同 token 或不同结果返回 REJECTED；key 已过期返回 MISSING。
- getIdempotency 只读取状态；TTL 到期后返回 ABSENT。

token 和 result 在 Redis 中使用 URL-safe Base64 编码，避免分隔符和 UTF-8 内容破坏状态。占用和完成转换都在 Redis Lua 脚本内完成，旧持有者不能覆盖后来重新占用的命令。pending TTL 是租约上限；业务执行时间可能超过它时，应延长租约或进入人工核对，不能让旧执行者继续写入完成结果。

这个接口解决的是“同一个命令的重试去重”。它不执行 Mongo/MySQL 写入，也不提供跨数据库事务；如果完成 Redis 记录后业务库写入失败，业务层仍要保存失败状态或进行补偿。Redis 驱动异常统一按 UNKNOWN 处理，不能因为客户端超时就直接换 token 重做非幂等操作。P3.4 的 ResilientRedisIdempotency 仅用相同 key、token 和 result 包装这三个可重放接口，配置与限制见 [存储韧性说明](storage-resilience.md)。

结果值上限为 16 KiB，TTL 必须为正数。RedisStore 的 OrderedExecutor 保证同一 key 的本地提交顺序；Lua 脚本保证多个进程之间的占用/完成条件检查原子化，但 Redis 单实例或当前部署的持久性、故障转移能力仍由运维配置决定。

示例：

~~~java
var claim = store.claimIdempotency("game:reward:req-100", "worker-a",
        Duration.ofSeconds(30)).toCompletableFuture().get();
if (claim.status() == RedisStore.IdempotencyStatus.ACQUIRED) {
    var status = store.completeIdempotency("game:reward:req-100", "worker-a",
        "{\"ok\":true,\"version\":7}", Duration.ofDays(1))
        .toCompletableFuture().get();
    if (status != RedisStore.CompletionStatus.APPLIED &&
        status != RedisStore.CompletionStatus.ALREADY_COMPLETED) {
        throw new IllegalStateException("idempotency completion was rejected: " + status);
    }
} else if (claim.status() == RedisStore.IdempotencyStatus.COMPLETED) {
    // 直接返回 claim.result()，不要再次执行业务。
} else {
    // IN_PROGRESS：等待、查询或交给业务补偿策略。
}
~~~

真实 Redis 测试覆盖竞争占用、完成结果重放、Unicode 结果、pending/result TTL、旧 token 拒绝和参数校验。 RedisStore 默认允许 Lettuce 自动重连；发生连接断开后的写入确认丢失时，返回结果仍按 UNKNOWN 处理并要求对账。仅测试故障代理使用的四参数构造器可关闭 autoReconnect，用于稳定注入“服务端已处理但客户端未收到确认”，不能把这个测试开关当作生产 exactly-once 保证。它不验证 Redis 重启、主从切换或网络分区；这些属于 P3.5 故障矩阵。