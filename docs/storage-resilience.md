# 存储重试、超时与熔断（P3.4）

ResilientExecutor 是 game-storage-core 中的异步调用治理组件。它不绑定某一种数据库，也不猜测业务操作是否安全。调用方必须为每次调用选择 RetryPermission.NEVER 或 RetryPermission.IDEMPOTENT，并提供异常分类器；只有分类器返回 true 且权限为 IDEMPOTENT 时才会重试。

## 执行顺序

每次调用按以下顺序处理：

1. 取得固定名称的熔断器许可。OPEN 状态快速拒绝；开放窗口到期后只允许一个 HALF_OPEN 探针。
2. 在启动操作之前预留一张超时定时票据。票据容量不足时操作不会启动。
3. 调用 Supplier 取得 CompletionStage。Supplier 必须快速返回，不能在调用线程阻塞数据库或修改 Actor 状态。
4. 成功时关闭熔断器并完成结果；失败时由分类器判断是否属于后端暂时故障。
5. 只有显式声明 IDEMPOTENT、属于可重试故障且未达到 maxAttempts 时，才按 initialBackoff 的 2 倍增长等待，最多到 maxBackoff。
6. 连续可重试故障达到 circuitFailureThreshold 后熔断；circuitOpenDuration 到期进入单探针半开状态。

ResiliencePolicy.defaults() 当前为最多 3 次尝试、单次 2 秒、50 毫秒起始退避、1 秒上限、连续 5 次故障熔断 5 秒。生产配置应按数据库超时、游戏请求 deadline 和峰值容量显式创建，不能把默认值当成性能承诺。

## 安全边界

超时仅表示框架停止等待该次结果，不能取消已经被驱动或数据库接受的副作用。迟到结果不会覆盖后续尝试的最终结果，但底层操作可能已经执行。因此：

- Redis 幂等 claim/complete 使用相同 key、token 和 result，可通过 ResilientRedisIdempotency 安全重放；相同 token 的 claim 会刷新 pending TTL 并重新返回 ACQUIRED，完成重放返回 ALREADY_COMPLETED。
- 普通 Redis INCRBY、发奖、支付、扣费和没有幂等键的 SQL 必须使用 NEVER。
- Mongo 增量只有在调用方保留完全相同的 expectedVersion、operationId 和载荷，并按 CAS 结果核对时才可能声明 IDEMPOTENT。不能重建版本或 operationId。
- 分类器只应把连接中断、明确的临时后端故障和 AttemptTimeoutException 归为可重试。参数错误、唯一约束、CAS 冲突、MISSING 和业务拒绝不应重试。
- 熔断器按后端或操作组使用固定名称，例如 redis-idempotency、mongo-player-write。禁止使用玩家 ID、房间 ID 或请求 ID；注册表有硬上限，超限会在操作启动前拒绝。

异常分类器本身抛错时，本次调用失败；半开探针会重新打开熔断器。不可重试的半开结果说明后端已经能够响应，因此关闭熔断器并把业务错误返回调用方。

## 容量、线程和关闭

每个执行器有 maxPendingTimers 和 maxCircuits 两个硬上限。一次在途尝试占用一张超时票据，等待退避也占用一张票据。容量不足会返回 RejectedExecutionException，不启动没有超时保护的新操作。

定时线程只负责触发超时和启动下一次异步尝试，不应执行阻塞数据库工作。操作的 CompletionStage 回调可能运行在数据库线程；游戏状态仍必须通过 ActorRef.pipe 回到所属 Actor。

close() 拒绝新调用，使正在等待超时或退避的 Future 明确失败，取消所有定时票据。已经提交给数据库的底层操作不能被撤销，其迟到完成不会计为成功。应用应先停止接收业务、等待已知安全的持久化流程，再关闭治理执行器和数据库连接。

Stats 提供 calls、attempts、retries、timeouts、successes、failures、circuitRejections、circuits、pendingTimers 和 closed。它是进程内累计指标，不是持久审计流水。

## Redis 实际接入

~~~java
var policy = new ResiliencePolicy(
    3, Duration.ofSeconds(1),
    Duration.ofMillis(20), Duration.ofMillis(200),
    5, Duration.ofSeconds(3));

try (var resilience = new ResilientExecutor(policy, 1, 1024, 32, "storage-resilience")) {
    var idempotency = new ResilientRedisIdempotency(
        redisStore, resilience, "redis-idempotency");

    var claim = idempotency.claim(key, token, Duration.ofSeconds(30))
        .toCompletableFuture().get();
    if (claim.status() == RedisStore.IdempotencyStatus.ACQUIRED) {
        // 执行业务；完成时必须继续使用相同 key、token 和 result。
    }
}
~~~

当前退避没有随机抖动，也没有跨节点共享熔断状态。多个进程会各自熔断；大规模同启恢复仍需在调用层分散启动时间。P3.5 将继续验证真实断连、超时、中间件重启和积压上限。