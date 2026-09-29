# 架构说明

## 分层

- `vendor/zfoo`：内置并可直接修改的 zfoo 协议、网络、事件和调度源码。
- `game-base`：对象、组件、工厂和基础数据结构。
- `game-runtime`：ActorSystem、Tick 调度、AOI 和运行时监督。
- `game-scene`：场景、Zone、移动复制和迁移边界。
- `game-storage-*`：Mongo、MySQL、Redis 和故障测试适配器。
- `game-transport-zfoo`：zfoo 编解码、四种网络协议、Session/Actor 路由和安全边界。
- `game-demo`：可运行的协议、存储和真实网络契约样例。

ServiceDirectory 位于 game-runtime，维护有界的角色/能力服务实例目录，使用租约心跳和过期剔除保护成员有效性，路由选择同时考虑能力、排空状态、权重和上报负载。GracefulDrain 位于同一运行时，使用请求准入 permit 统计在途工作；进入排空后拒绝新工作，按 deadline 等待旧工作，最后逆序关闭资源并报告超时和关闭异常。 HealthRegistry 提供有界的异步健康检查、单项超时隔离、稳定排序和整体状态聚合。 RuntimeMetrics 提供有界并发计数器、仪表快照和 Prometheus 文本导出；GameServerConfig 负责进程级配置默认值与范围校验。 `SecretResolver` 负责从注入值、环境变量和系统属性读取外部密钥，不把密钥放入配置记录或异常文本。 ServiceNodeController 把本地 ServiceDirectory、共享 LeaseStore 和 GracefulDrain 组成一个节点生命周期边界，存储实现可以通过 Redis 或 MySQL 适配。 ServiceDirectorySnapshotApplier 作为 watcher 与远端 ServiceDirectoryView 之间的统一结果边界。 ServiceDirectorySnapshotTracker 只有在调用方声明某些角色快照完整时才删除缺失实例，避免有界分页误删成员。 RedisServiceDirectoryBridge 和 MySqlServiceDirectoryBridge 负责把 watcher 快照接入该策略；Redis 未变化的租约也会刷新读侧有效期。P5.2 又加入代际、随机租约令牌、下线墓碑和路由 fencing，使迟到心跳、旧连接和旧地图路由不能复活已撤销实例。它仍是进程内注册权威；`ServiceDirectoryView` 单独承载 Redis/MySQL 快照合并，拒绝旧代际和错误 token，并提供远端实例的能力、权重、负载选择；租约中的 load/draining 元数据会随 heartbeat 一起传播。

## 关键取舍

业务逻辑以 Actor 串行执行，Session 先绑定临时 sid Actor，认证成功后由 GameSessionBinder 晋级到 uid Actor。旧连接使用 generation 和 session identity 检查，避免断线后的迟到消息进入新连接。

存储接口不把业务对象绑定到 ORM。组件可以映射到多个文档或表，DirtyDocumentSet 合并字段级变更；Mongo 支持 CAS，MySQL 使用显式 SQL，Redis 适合缓存和短期状态。RedisServiceLeaseRegistry 以带 TTL 的 token 条件写入服务实例，RedisServiceDirectoryWatcher 通过有界轮询快照触发变更回调；MySqlServiceLeaseRegistry 以显式 SQL 和 SELECT FOR UPDATE 完成过期实例替换。两种适配器都拒绝旧代际续租或释放新代际，并在 Redis 记录或 MySQL 行中保留 load/draining 状态；MySQL 初始化会为旧表补齐元数据列。

网络层统一执行入站字节/包预算、出站队列预算和状态包合并。可靠 UDP 提供有界 ACK、有限重传、乱序/去重、分片重组、CRC32C 和会话代际，但身份认证和加密由 TLS/mTLS 与 P4.7 认证层负责。

RpcPendingCalls 位于 game-runtime，按请求 ID 有界保存 pending call，使用绝对 deadline 和控制面过期清理，并把已完成 ID 保留在有限窗口内过滤重复响应。RpcIdempotencyRegistry 按稳定 command ID 合并重试，只执行一次原始操作，并在保留窗口内复用结果。RpcCallExecutor 统一执行总 deadline、单次超时、有限重试和按目标服务熔断，重试不会改变 command ID。GameRpcClient 位于 game-transport-zfoo，统一传输适配器的请求入口；RpcRequestEnvelope/RpcResponseEnvelope 携带 destination、commandId、deadline 和已注册 zfoo payload，ZfooRpcPendingResponses 负责响应关联、重复过滤和 deadline 清理，ZfooRpcServerHandler 负责服务端分流与回包，ZfooRpcClientAdapter 复用 Session、UDP attachment 和 RpcCallExecutor，统一发送、重试与 typed response 解码。具体协议只负责编码、发送和响应解码。ReplayGuard 按身份隔离 nonce，校验时间窗、未来偏差、TTL 和总容量。GameAuthenticationService 在此基础上完成账号查找、常量时间密钥比较和 uid Actor 晋级；game-demo 已提供 AuthenticationRequest/AuthenticationResponse 的 TCP、WebSocket、UDP 真实契约；权限策略和跨服 RPC 属于后续阶段；CrossProcessMigrationCoordinator 已提供跨 Game 节点迁移的状态机边界，释放/提交不确定时保留恢复态。MigrationJournal 定义带版本 fencing 的持久化接口，RedisMigrationJournal 和 MySqlMigrationJournal 已提供创建、CAS 状态推进、恢复态列表和条件删除；协调器已接入日志生命周期，`CrossProcessPlayerMigration` 已把玩家节点 RPC 操作和目标提交后的路由发布接入协调器；`ZfooPlayerMigrationNode` 将六个迁移阶段编码为稳定 commandId 的幂等 zfoo 命令，`ZfooPlayerMigrationHandler` 负责服务端解码与 typed reply。`RuntimeEventLogger` 提供有界结构化生命周期事件，并对 token、secret、password 和 key 字段自动脱敏；跨 JVM 本机和跨容器演练均已通过；生产部署仍需按实际网络拓扑配置监听地址。

## 边界

一个 Actor 不等于一个 OS 线程，单条长任务仍会占用 worker。直接调用 zfoo Router.send 会绕过 ZfooSender 的出站预算。内存存储后端用于开发，不模拟数据库副本集、事务和完整 BSON 转换行为。

MySqlServiceDirectoryWatcher 按角色轮询显式 SQL 租约；RedisServiceDirectoryWatcher 使用有界 SCAN，并在每次成功轮询刷新读侧租约期限。两者的 Bridge 都把可能截断的快照限制为只更新，只有完整快照才允许缩容删除。
