# ServerFrame → GameFrame TODO

本清单按依赖顺序推进。项目使用 JDK 25，zfoo 源码位于 `vendor/zfoo`，所有改造在当前项目内完成。

当前里程碑：P0、P1、P2、P3、P4.1、P4.2、P4.3、P4.4、P4.5、P4.6、P4.7、P5.1、P5.2 已验收，P5.3 已完成 RPC 执行、zfoo envelope、响应关联、服务端分流、Redis/MySQL 租约适配、Redis 变更监听、远端目录合并、ServiceDirectory RPC 路由，以及租约 load/draining 跨进程发布；Gate/Game 控制面样例已覆盖扩容、心跳失效后的递增代际重新注册、恢复路由、排空和缩容；Redis/MySQL 双节点并发压力验证已完成。已提供 CrossProcessPlayerMigration 玩家迁移业务门面、zfoo 玩家迁移客户端节点和服务端处理器；双节点 TCP 迁移演练已通过；跨 JVM 本机和跨容器部署演练已通过。

## P0 基线与基础工具

- [x] 建立源码基线、能力对照、分阶段计划及验收规则。
- [x] 完成 Factory/ObjectRegistry、ObjectPool、组件注册和基础生命周期能力。
- [x] 固定 JDK 25、Maven 构建和 zfoo 源码内置结构。

## P1 场景与战斗运行时

- [x] ActorSystem、对象/组件模型、GridAoi、FixedStepLoop 和共享 Tick 调度。
- [x] MovementReplicator、场景进入/离开、快照与增量状态包。
- [x] 有界邮箱、监督策略、事件句柄和跨 Actor 投递边界。

## P2 Actor、场景与迁移

- [x] Zone、玩家 Actor、串行 mailbox、迁移准备/确认/提交/取消。
- [x] 迁移超时、重复 ACK、晚到消息、邮箱拒绝和 Actor 停机故障测试。
- [x] 组件类型工厂及业务 ObjectManager 接入。

## P3 存储与一致性

- [x] Mongo、MySQL、Redis 的独立接口和适配模块，不依赖 ORM 作为业务边界。
- [x] DirtyDocumentSet，支持组件拆分、脏字段合并、局部更新和全量保存。
- [x] Mongo 单文档 CAS、批量逐项结果、游标分页和显式字段索引。
- [x] 内存后端、故障注入后端和存储契约测试。

## P4 网络与安全

- [x] P4.1 统一 Session/Actor 路由、旧连接代际隔离。
- [x] P4.2 登录绑定、入站字节/包限制、心跳超时和真实 zfoo TCP 集成。
- [x] P4.3 出站队列预算、慢客户端拒绝和状态包合并。
- [x] P4.4 TCP、UDP、HTTP、WebSocket 真实客户端契约；UDP 按远端地址维护有界逻辑 Session。
- [x] P4.5 可靠 UDP：CRC32C、ACK、有限重传、乱序、去重、分片、会话代际、HELLO 首包握手和远端重连后的旧包隔离。
- [x] P4.6 TLS/mTLS、主机名校验、证书错误拒绝、TCP TLS 和真实 WSS 握手。
- [x] P4.7 防重放与密钥比较：ReplayGuard、GameAuthenticationService 和 AuthenticationRequest/AuthenticationResponse 已完成；真实 TCP、WebSocket、UDP 客户端均验证错误密钥、认证成功、uid Actor 绑定和重复 nonce 边界。账号凭据通过外部查询函数注入，生产数据库适配不耦合传输层。

## P5 服务发现与 RPC 治理

- [ ] 服务角色、能力标签、负载感知路由和注册中心适配；`game-runtime` 已完成进程内 `ServiceDirectory`、代际租约和令牌基础；`game-storage-redis` 已提供 Redis token 条件租约发布/续租/释放、有界实例快照和变更监听；`game-storage-mysql` 已提供显式 SQL、行锁和代际 fencing 的租约适配。`ServiceDirectoryView` 已完成远端快照合并、代际/token fencing、过期清理和负载感知选择，`ServiceDirectoryRpcRouter` 已完成按目录重选和失败实例冷却；Gate/Game 扩缩容样例已加入 game-demo，RedisServiceDirectoryBridge 和 MySqlServiceDirectoryBridge 已完成自动快照接线，Redis 和 MySQL 双节点多客户端目录演练均已通过；跨后端压力验证已完成：Redis/MySQL 双节点并发负载发布与目录轮询均通过。
- [x] P5.2 进程内治理基础：代际与随机租约令牌、下线墓碑、地图路由撤销、排空保持和 fencing 已完成；P5.3 已补齐 Redis/MySQL 租约的 load/draining 跨进程传播。
- [x] RPC deadline、有限重试、熔断、重复响应过滤和幂等；`game-runtime` 已完成有界 pending call、绝对 deadline、重复响应过滤和幂等请求合并基础，RpcCallExecutor 已提供统一 deadline、有限重试和按目标熔断；`game-transport-zfoo` 已提供 `GameRpcClient` 传输门面和 `RpcConnectionPool`：按目标地址限制连接数与单连接并发，故障连接摘除后下一次调用自动重连；Gate/Game 控制面样例已验证扩缩容、失联重注册和恢复路由；ServiceNodeController 已支持失败后递增代际重新注册、清除 draining 并恢复心跳。CrossProcessMigrationCoordinator 已接入可选 MigrationJournal，Redis/MySQL 适配器和恢复态生命周期已验证；CrossProcessPlayerMigration 已定义实际玩家迁移业务契约；ZfooPlayerMigrationNode/ZfooPlayerMigrationHandler 已完成协议接线、服务端幂等合并和重复命令测试，双节点 TCP 演练已通过，跨 JVM 本机和跨容器部署演练已通过。
- [ ] Gate/Game/控制面扩缩容、重连、重新注册和跨进程迁移；控制面扩缩容、失联重注册、Redis/MySQL 多进程压力、迁移日志恢复态和 zfoo 玩家迁移节点契约已通过，跨 JVM 本机和跨容器玩家迁移演练已通过。

- [x] P5.3 跨进程迁移协调器：加入冻结、目标准备、源释放、目标提交、取消和 `RECOVERY_REQUIRED` 恢复态；超时期间的远端操作未落定时保持恢复槽位，并限制同一 Actor 的并发迁移。
## P6 运维与配置

- [x] 配置校验、密钥注入、日志/指标；`GracefulDrain` 的优雅排空和 `HealthRegistry` 的健康聚合已完成，GameServerConfig 已完成基础配置解析与范围校验，RuntimeMetrics 已提供有界计数器、仪表和 Prometheus 文本导出；`SecretResolver` 已支持注入值、环境变量前缀和系统属性回退，返回 char[] 时提供防修改副本；`RuntimeOperations` 已把 HealthRegistry、RuntimeMetrics 和 GracefulDrain 合并为统一运维快照并提供 Prometheus 文本输出；deploy/compose.yaml 已提供 MongoDB、Redis、MySQL 健康检查。RuntimeEventLogger 已提供有界结构化事件、敏感字段脱敏和 sink 故障隔离；JDK 25 多阶段 Dockerfile 已提供生产镜像入口。
- [ ] 发布包、生产容器模板、压测基线和故障演练记录；开发依赖 compose 与运行说明已完成。

证据见 [验证记录](docs/validation.md)，架构取舍见 [架构说明](docs/architecture.md) 和 [迁移计划](docs/serverframe-port-plan.md)。