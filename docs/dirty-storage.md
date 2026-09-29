# 组件拆分存储与脏数据合并（P3.1）

DirtyDocumentSet 位于 game-storage-core，建立在现有 DocumentStore 上。它用于合并普通状态写入，
不依赖 ORM，不自动扫描 Java 对象字段。目前可直接使用 MemoryDocumentStore 和 MongoDocumentStore；
MySQL 保留显式 SQL/事务接口，Redis 保留原子命令接口，二者没有被强行转换成文档存储。

## 数据布局与接入

一个逻辑对象可以打开 core、bag、quest 等组件，每个组件分别维护数据库版本、已确认快照和本地脏状态。
当前布局为同一 section/collection 的多个文档，ID 使用“实体 ID 长度:实体 ID:组件名”避免组合歧义。
通过 key(component) 取得实际 StoreKey，不应手工拼接其 ID。
不同库、不同 collection 或不同持久化策略可以使用独立缓冲器；跨组件/跨缓冲器都不是原子事务。

~~~java
// 在所属 Actor 内创建；数据库回调只更新缓冲器的同步元数据，不接触 GameComponent。
var dirty = new DirtyDocumentSet(store, "player_component", playerId,
    new DirtyDocumentSet.Limits(8, 64, 8_388_608, 1_048_576, 1024),
    actor::requireCurrent);
var loading = dirty.open("core");
// 异步加载完成后，在 Actor 内继续：
dirty.markReplace("core", Map.of("gold", 100L, "level", 1L));
dirty.markPatch("core", new DocumentPatch(Map.of(), Set.of(), Map.of("gold", 1L)));
var flush = dirty.flush();
~~~

示意代码的后续步骤必须在 loading 完成后投递回 Actor，不能在 Actor 内 get/join。
open() 同名并发调用共享一次加载；新文档必须先用全量数据创建，局部更新不隐式 upsert。
mark 方法是同步的本地校验与入队，成功后再修改对应业务组件字段，避免容量拒绝后业务状态与存储缓冲脱节。
快照和 Patch 会冻结数据；数据库线程不会读取可变游戏对象。

localView() 返回本地修订号与包含未提交修改的不可变映像。
current() 只返回数据库已确认映像：写入结果未知时两者可能不同，业务必须检查 failure()。
需要保存此前读取的完整业务快照时，调用 markReplace(component, expectedRevision, data)，
避免旧快照覆盖之后的新修改。无修订号重载表示调用者明确提供当前完整状态，不适合提交过期缓存。

## 合并与保序

- 相邻未封存的同字段 set 保留最新值，整数 increment 在安全表示的范围内合并增量。
- 相邻 Patch 的非重叠字段可以合并；set/unset/inc 的组合遵循已验证的顺序语义。
- 父子路径修改保留先后顺序，不能放进同一 Mongo 更新；可能影响中间空父对象的 set/inc → unset 也保留顺序。
- 每次修改先验证本地结果，非对象父节点、null/非整数字段自增、整数溢出都会在入队前拒绝。
- 新全量快照覆盖该组件所有尚未封存的修改；已被 flush 捕获的操作无法被修改或替换。
- 每个组件只发送一个在途命令，确认后才按新的版本发送下一条；不同组件可以并行。
- 一批中前几次成功、后一次失败时，已确认版本和快照保留，不会回退或重复提交成功部分。

flush() 封存调用时的边界，返回逐组件 attempted/applied/status/error。
调用期间的新修改属于后续批次。已有 flush 未结束时再次调用会复用该边界，
**不会扩大为覆盖后来修改的屏障**；需要等待其完成后再次 flush，或使用 flushAndClose。
FlushResult.complete() 只表示该次捕获的批次全部确认，不表示本地永远没有新脏数据。

这是状态合并器，不是交易账本。合并会减少数据库版本推进次数，不保留每次中间业务操作的审计记录。
充值、交易、扣费、稀有物品归属等操作应走明确的持久命令和幂等流水，不能依赖延迟刷盘。

## 失败与重试

失败组件保留原队列并暂停新修改和后续自动写入。其他组件的成功结果不会被丢弃。
周期 flush 再次遇到失败组件，只报告故障，不悄悄重发。

- NOT_EXECUTED：数据库执行器入队前拒绝。
- UNKNOWN：异常可能发生在数据库已经写入、但确认尚未交付之后。
- CONFLICT / MISSING：版本或文档归属需要业务核对。
- INVALID：本地准备命令失败，需要应用检查状态或配置。

NOT_EXECUTED / UNKNOWN 可以由业务显式 retryFailed(component)，随后再次 flush。
重试保留同一个 WriteCommand、expectedVersion、operationId 和参数指纹，禁止重新生成版本后再做一次增量。
真实 Mongo 测试在已成功写入后注入确认丢失；重试返回 ALREADY_APPLIED，金币不会重复增加。
如果其他写者已经推进版本，重试仍会冲突；此时需要应用重新加载和核对，不自动覆盖。

P3.4 已提供通用的有限重试、退避、超时与熔断组件，但 DirtyDocumentSet 不会自行开启自动重试；调用方仍须按失败状态核对并显式决定。详见 [存储韧性说明](storage-resilience.md)。Redis TTL 幂等记录也不是永久审计账本。
未提交队列和固定命令只在当前进程内保存；进程崩溃后的恢复需要持久化日志或业务流水。
UNKNOWN 的重试能力依赖后端遵守现有 DocumentStore 的 CAS/操作标记契约。

## 周期刷盘与退出

已有 SharedTickScheduler + FixedStepLoop 可组合使用，不额外为每个玩家创建 timer 线程：

~~~java
var loop = new FixedStepLoop(actor, Duration.ofMillis(100), 1, nanos ->
    dirty.flush().whenComplete((result, error) -> {
        // 此处可能是数据库线程；只记录线程安全指标，或重新投递回 Actor。
        // 必须检查 result.complete() 和逐组件失败；不要把入队当作持久成功。
    }), sharedTimer);
~~~

正式应用通常每个房间/分片统一调度一组持久化对象，避免为每个组件单独注册周期任务。
示例用 10ms 仅为快速验证，实际周期应由允许丢失的状态时间窗口、数据库负载和关键数据策略决定。

退出顺序：停止新业务 → 停止周期循环 → 在 Actor 内调用 flushAndClose →
在 Actor 外或异步流程等待结果 → 释放组件/对象并停止 Actor → 关闭数据库。
flushAndClose 立即冻结新修改，等待当前批次，再排空它之后已经排队的修改。
成功后关闭缓冲器；失败时保留故障与队列，维持禁止新修改状态，可以显式核对/重试后再尝试退出。
有未完成加载时拒绝启动关闭，需先处理加载结果。

close() 不发起 I/O，也不会悄悄丢弃脏数据；存在脏操作、在途写入或加载时抛错。
不能在 Actor.onStop 才发起依赖该 Actor 继续处理业务的异步刷盘。
没有把超时等同于取消写入，也没有承诺强制终止阻塞的驱动；驱动超时和自动恢复策略继续由后续阶段完善。

## 容量和性能范围

Limits 分别限制组件数、未确认操作数、累计逻辑载荷字节、单文档逻辑字节、单 Patch 路径数。
入队时预留不可变命令及在途结果映像的预算；替换、合并和非法数据拒绝不会破坏原队列或计数。
字节数是保守逻辑估算，**不是 JVM 堆占用或 BSON 序列化长度的精确值**，最大嵌套深度为 64。

这版每次修改需要投影本地组件映像，成本与组件文档大小有关。
应将高频小数据（位置/状态）、低频大数据（背包/任务）合理拆开；
超大 List 仍整体替换，尚无 arrayFilters 或增量容器 codec。
当前验证证明减少写入次数与失败语义正确，没有宣称达到某个吞吐/P99，也没有完成持续压测。

## 可运行示例

~~~powershell
java -jar game-demo/target/game-demo-0.1.0-SNAPSHOT.jar --persistence
~~~

PersistenceDemo 使用真实 Actor 和两个 GameComponent：
初始化 core/bag；100 次金币变化合并为一个 Patch；与背包更新一起周期刷盘；
随后全量 checkpoint 与一次局部更新保序执行，在 Actor 停止前排空。
整体写入 6 次：初始化 2 次、周期批次 2 次、退出批次 2 次。

~~~text
Persistence sample: Summary[mergedEdits=100, pendingBeforePeriodic=2, writeCalls=6, coreVersion=4, bagVersion=2, gold=203, slots=4, dirtyAfterShutdown=0]
~~~

测试覆盖 300 次固定种子操作与逐条执行的内存参考后端对照；
Mongo 使用真实实例与随机测试库，确认丢失通过成功写入后的回调故障注入，不代表已经完成网络断连/副本集重启测试。
