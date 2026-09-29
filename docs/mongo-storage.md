# Mongo 批量结果、分页与索引（P3.2）

MongoDocumentStore 保留 DocumentStore 的单文档 CAS 接口，额外提供显式批量提交、按 ID 分页和单字段索引。业务状态仍使用不可变 WriteCommand，不依赖 ORM。

## 批量提交

writeBatch(List<BatchItem>) 返回按输入顺序排列的 BatchItemResult，其中 index 对应原输入位置，key 对应文档。每项分别给出 WriteResult 或 error；complete() 仅在每项均为 APPLIED / ALREADY_APPLIED 时成立，failures() 返回其余项。

这是**应用层逐条写入的汇总接口**，当前没有使用 Mongo bulkWrite，不减少数据库往返次数，也不提供批次事务。高频状态的写入次数主要由上层 [DirtyDocumentSet](dirty-storage.md) 合并降低；原生 bulkWrite 的吞吐优化需要独立设计和基准测试，尚未实现。

- 同一 StoreKey 按实际入队顺序串行；不同 key 可以并行。批次之间不保证整体原子入队。
- 单项版本冲突、文档不存在、队列拒绝或驱动异常不会取消其他项。同一 key 的后续项也会继续，只有版本匹配才可能执行；需要“失败立即停止后续写入”的业务应逐项等待，或使用 DirtyDocumentSet。
- maxBatch 默认 256，同时限制每批项数和每页记录数；构造时可配置。队列仍受各 lane 容量限制，一批可能部分入队成功。这个限制不代表载荷字节或 JVM 堆内存上限。
- 提交前复制并检查整个输入列表；WriteCommand 在构造时递归冻结数据。null 元素或批次超限会在任何一项提交前拒绝。
- 批次完成只表示每项已有结果，必须检查每项状态。取消调用方等待不等于撤销已提交写入。

保留原输入列表，利用 index 精确关联失败项；重试必须保持原 operationId、expectedVersion 和载荷。NOT_EXECUTED 表示入队前拒绝；UNKNOWN 表示本接口不能确认整体写入结果，应核对原命令，不能换版本重新执行自增。最近一次原命令可以识别为 ALREADY_APPLIED；更老命令会冲突，并非永久幂等账本。禁止无差别重试整个批次。

唯一索引冲突会保留 Mongo 错误码和原始异常；对于新 ID 违反另一个唯一索引的情况，不再误报 MISSING。当前驱动异常统一保守归为 UNKNOWN，不能仅凭该分类判断是否适合重试；例如 11000 应处理业务唯一性冲突，不能自动循环提交。

## 分页

page(section, afterId, limit) 适用于本框架写出的、使用字符串 _id 和版本封装的集合。首屏 afterId 为 null；后续使用返回的 nextCursor（上一页最后的 ID，排他边界），无需自行解析或修改。它是 ID 游标，不是加密或签名令牌。

排序显式使用 simple collation；每次最多读取 limit + 1 条，返回最多 limit 条。只有实际读到后一条才提供 nextCursor，因此最后一页即使刚好满页，hasNext() 也为 false。查询设置五秒服务端执行上限；队列等待不计入这个上限。页数限制不能代替文档大小限制。

返回的是完整 Snapshot，不能把投影查询结果误当作可全量替换的对象。当前没有通用条件查询 DSL。分页不是跨页一致性快照：并发创建/删除可改变后续页；游标之前新建的文档也可能不会被本轮扫描看到。不适合直接作为一致性备份方案。自定义非 simple collation 的集合应自行验证查询计划。

page、ensureIndex 使用独立调度 key，不充当所有写入的屏障。依赖写入结果的查询应先等待并检查该写入/批次完成。

## 索引配置

ensureIndex(IndexSpec) 显式指定 section、字段路径、升降序及 unique。业务字段位于 data 下，例如 data.level；允许的路径段与 Patch 一致。当前是单字段索引配置，不包含复合索引、TTL 或后台迁移工具。

索引名包含可读前缀和完整配置的 SHA-256，避免 data.a_b 与 data.a.b、超长相同前缀或不同方向发生命名覆盖。重复提交同一配置可复用索引；服务端已有索引与配置不兼容时保留错误，不静默修改或删除已有索引。生产环境应在接流量之前完成索引检查；大型集合建索引应单独安排。

## 调用示例

以下代码在应用启动或管理线程中执行；不要在 Actor/Netty 线程阻塞等待。MongoDocumentStore 由调用方创建、关闭，数据库应为开发库。示例可重复运行，相同创建命令会识别为 ALREADY_APPLIED；若文档已经被其他业务修改，按冲突处理。

~~~java
var spec = new MongoDocumentStore.IndexSpec("example_profile", "data.level", true, false);
store.ensureIndex(spec).toCompletableFuture().get(15, TimeUnit.SECONDS);

var input = List.of(
    new MongoDocumentStore.BatchItem(new StoreKey("example_profile", "p100"),
        WriteCommand.create("example-create-p100", Map.of("level", 1L))),
    new MongoDocumentStore.BatchItem(new StoreKey("example_profile", "p101"),
        WriteCommand.create("example-create-p101", Map.of("level", 2L))));
var batch = store.writeBatch(input).toCompletableFuture().get(15, TimeUnit.SECONDS);
if (!batch.complete()) {
    // 此处交给应用处理，保留 input 和每项结果；不要整批自动重试。
    throw new IllegalStateException("Batch has failures: " + batch.failures());
}
String cursor = null;
do {
    var page = store.page("example_profile", cursor, 100)
        .toCompletableFuture().get(15, TimeUnit.SECONDS);
    page.items().forEach(row -> System.out.println(row.id() + ": " + row.snapshot()));
    cursor = page.nextCursor();
} while (cursor != null);
~~~

MongoBatchQueryIT 在真实 MongoDB 随机数据库中覆盖七组场景：逐项结果与混合失败、分页边界、索引名称冲突与实际定义、唯一约束、同 key 连续版本及精确重放、输入冻结与提交前校验、关闭后逐项拒绝。测试结束在 finally 中清理各自数据库。全量结果见 [验证记录](validation.md)。副本集故障、真实网络断连、原生批量吞吐和 P99 仍待后续验证。