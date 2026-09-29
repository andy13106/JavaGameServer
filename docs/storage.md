# 存储语义

## 所有权与执行线程

Actor 只维护自己的游戏状态；数据库工作线程不读取/修改可变游戏对象。
WriteCommand、Snapshot、DocumentPatch 构造时递归复制并冻结数据。初版支持 String、Boolean、Integer、
Long、有限 Double、null、字符串键 Map 和 List；日期建议保存时间戳，ObjectId/Decimal128/二进制等需增加显式 codec。

OrderedExecutor 将 key 散列到固定数量的单线程工作队列，每条队列有上限。
同一 StoreKey 的实际提交顺序保持串行，不同 key 可并行；散列冲突可能使不同玩家互相等待。
这是第一版的简单实现，不是动态 work-stealing 存储调度器。Future 回调可能运行在数据库线程，状态修改必须经 ActorRef.pipe 回投。
MySQL SqlWork 也只在数据库线程执行，应在回调内部复制读取结果，关闭 Statement/ResultSet，禁止泄漏 JDBC 资源给 Actor。

## Patch 与 Snapshot

Patch 支持嵌套对象路径的 set/unset 和整数字段 increment。路径不允许索引数组或包含 $，同一 Patch 不能同时修改父子路径，
不能对一个字段同时 set 和 increment。List 当前整体替换；物品数量很多时可按物品或背包分区拆文档，
也可随后提供受约束的 Mongo arrayFilters 适配，不能绕过版本链路偷偷写同一文档。

Snapshot 是一个完整存储单元，replace 会删除快照未包含的原有业务字段。
不能把字段投影或只加载了一部分的对象当作完整快照保存。

expectedVersion 是数据库并发条件：_id 和 _version 同时匹配才写入。
版本不匹配时返回 CONFLICT，未找到返回 MISSING；版本冲突不自动 reload 然后覆盖。
读取可能在检查冲突后观察到更高版本，这不改变本次写入未匹配的事实。

## 重试与未知结果

WriteResult:
- APPLIED：该次写入已获得驱动配置的确认。
- ALREADY_APPLIED：当前文档恰好是同一 expectedVersion、operationId、命令指纹写出的下一版本。
- CONFLICT：不能应用，必须由状态所有者协调。
- MISSING：文档不存在，不对局部更新自动 upsert。

数据库文档只保存**最后一次操作**标记。这能安全识别最近一次相同命令重发，
但不是永久交易流水，也不保证老请求在任意时间重放都能查回原结果。
更晚的版本已经提交时，旧命令返回冲突；绝不能换 expectedVersion 再把 $inc 重做一遍。
相同 operationId 换参数也不能被当作成功重试。指纹是完整命令（包含版本）的 SHA-256。

StorageException.NOT_EXECUTED 表示在执行器入队前拒绝，操作没有开始。
StorageException.UNKNOWN 表示驱动异常可能发生在服务端提交之后。框架不会自动重试非幂等业务操作；有限重试、超时和熔断的使用方式见 [存储韧性说明](storage-resilience.md)。
Mongo 驱动自身的 retryable writes 与业务重试是不同层次；仍须保留命令的版本和标识。
支付、交易和跨文档资产变更需要单独的持久业务流水/事务设计，不由这个轻量文档协议代替。

Mongo 默认 primary 读取、majority 写确认以及有界等待。standalone 开发环境不提供副本集故障恢复保证。
进程内存未提交的变化没有持久性；flush 是队列屏障，仅覆盖同 key 在它之前提交的工作，
不会捕获业务对象未生成的 Patch，也不会重报之前已经返回的冲突。必须先检查各写入结果。

## 关闭与异常

先停止新业务，再等待持久化请求和 Actor 回调完成，然后关闭数据库执行器/连接，最后关闭 ActorSystem。
执行器 close 会排空已接收任务；若超时会报告失败，不宣称任务被取消。
ActorSystem.close 会拒绝新的提交并显式失败尚未回来的外部回调；因此它不能代替业务 flush。
Actor 单条处理异常会交付给调用者，后续消息继续。涉及资产的处理器要先校验再修改，或在业务层转入故障状态。
PlayerService 示例在冲突或未知存储结果后停止后续写入，避免带着过期版本继续覆盖。

## 高频状态写入

P3.1 已提供 DirtyDocumentSet：按逻辑组件拆分文档、合并显式脏字段、封存不可变在途批次、
逐组件反馈、失败暂停和保序退出刷盘；用法及完整限制见 [组件存储说明](dirty-storage.md)。
使用方仍需显式标记变化，不扫描任意 Java 对象，也不自动合并需要持久流水的交易操作。
同一组件应由一个状态所有者维护；不要把整玩家多文档 flush 称为原子快照。

## Mongo 专用接口

P3.2 增加 writeBatch 逐项结果、page 游标分页及 ensureIndex 单字段索引配置。批量仍逐条执行，不等同于 Mongo 原生 bulkWrite；错误分类、同 key 失败后行为、查询顺序及完整调用示例见 [Mongo 说明](mongo-storage.md)。

Redis 幂等占用/完成接口与边界见 [Redis 幂等说明](redis-storage.md)。它只保护单个业务命令的重复提交，不构成跨数据库事务。
