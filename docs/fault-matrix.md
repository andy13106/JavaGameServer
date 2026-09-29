# P3.5 数据库故障矩阵进度

本阶段使用隔离的临时 Docker 实例，不触碰现有 `orbais-mysql` 容器或业务数据。停止/恢复测试使用 MongoDB 8.0、Redis 7.4、MySQL 8.4；协议级测试使用同类版本，测试结束后已删除临时容器。

## 已验证

| 后端 | 隔离实例停止/恢复 | 协议级写确认丢失 | 证据 |
|---|---|---|---|
| MongoDB | 通过：停止后有界失败、同 key 第三项 `NOT_EXECUTED`、恢复后新 store 可读 | 通过：代理丢弃写响应；独立查询发现文档已落库；客户端返回 `StorageException.UNKNOWN`；新 store 可读 | `MongoFaultMatrixIT`、`ProtocolFaultMatrixIT` |
| Redis | 通过：停止后在途 get 失败、恢复后新连接 SET/GET 成功 | 通过：故障连接关闭自动重连后，代理丢弃 SET 响应；独立连接读到值；客户端返回 `StorageException.UNKNOWN`；新 store 可读 | `RedisFaultMatrixIT`、`ProtocolFaultMatrixIT` |
| MySQL | 通过：停止后连接失败、队列第三项 `NOT_EXECUTED`、恢复后新连接执行成功 | 通过：代理丢弃 INSERT 响应；独立连接读到行；客户端返回 `StorageException.UNKNOWN`；新 store 可读 | `MySqlFaultMatrixIT`、`ProtocolFaultMatrixIT` |

BacklogMatrixIT 已覆盖三种后端 capacity=1/2/4 的停机积压矩阵，并验证双 lane、每 lane capacity=1 的并行积压；每组都确认有界失败和至少一个 NOT_EXECUTED，拒绝数量不超过配置容量；3 项均通过，摘要见 p35-matrix.log。

Redis 还增加了停机队列上限测试：lanes=1、capacity=1 时突发请求中至少一项在执行前返回 NOT_EXECUTED，其余已接受请求在有界等待内失败；测试不依赖固定请求序号，避免停机竞态造成误判。

三种后端各增加了 1 项重启持久性测试：写入成功后 stop/start 同一个临时容器，再用新的 store/连接读取，MongoDB 文档、Redis 值、MySQL 行均保留。RecoveryCycleMatrixIT 又对每个后端执行 2 轮连续 stop/start，3 项均通过，证据见 p35-cycles.log。

测试专用模块为 `game-storage-fault-test`。其中 `TcpFaultProxy` 只在测试代码中使用：请求继续转发到真实服务端，服务端响应被读到后丢弃并关闭连接，因此可以证明“服务端已处理”和“客户端未收到确认”是两个独立事实。

协议级代理还验证了服务端响应 stall：MongoDB 12 秒、MySQL 12 秒、Redis 7 秒；ProtocolFaultMatrixIT 共 6 项通过。三种驱动均在响应迟到前返回 StorageException.UNKNOWN，独立连接确认写入副作用已经存在。LateResponseRetryMatrixIT 又对三种后端各执行 1 次 stall + attempt timeout + 幂等重试，3 项通过，迟到的第一次成功响应没有覆盖重试结果，证据见 p35-retry.log。

Redis 的故障测试显式使用 `new RedisStore(uri, lanes, capacity, false)` 关闭故障连接的 Lettuce 自动重连；正常构造器仍保持自动重连默认值。这个开关只用于把确认丢失边界稳定地暴露给测试，不能把重连成功误判为 UNKNOWN。

ResourcePressureMatrixIT 验证了真实 MongoDB 上的 DirtyDocumentSet 逻辑字节水位：超限替换被拒绝，本地旧状态保持不变且仍能成功 flush。联合积压测试同时阻塞 MongoDB、Redis、MySQL 的首个响应；capacity=2 时每个后端接受 1 个执行中和 2 个排队请求，额外 2 个请求均返回 NOT_EXECUTED，连接恢复后已接受请求全部完成。2 项通过，证据见 p35-pressure.log。

## 验收结论

P3.5 已完成停止/恢复、确认丢失、服务端 stall、幂等重试、多轮重启、队列容量、多 lane、连接池与逻辑内存水位矩阵，现已验收。

在这些项目完成前，`StorageException.UNKNOWN` 只表示需要对账，框架不会把它宣传成 exactly-once，也不会自动重复非幂等写入。
