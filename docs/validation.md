## 2026-09-29：跨容器玩家迁移演练

新增 `PlayerMigrationContainerExerciseIT` 和 `deploy/Dockerfile.runtime`。使用 JDK 25 runtime 镜像启动两个独立 GameFrame 容器，节点监听 `0.0.0.0:9001` 并映射到宿主机 39101/39102；测试通过真实 TCP 完成 freeze、prepare、release、commit，容器日志确认四个阶段均执行，目标提交后发布路由。跨容器测试 1 项通过。
## 2026-09-29：跨 JVM 玩家迁移演练

新增 `MigrationNodeMain` 独立 Game 节点入口和 `PlayerMigrationCrossJvmExerciseTest`。测试进程启动两个独立 JDK 25 子 JVM，分别监听随机 TCP 端口；父进程通过真实 socket 和 zfoo RPC 执行冻结、准备、释放、提交，确认目标提交完成后才发布 `game-b` 路由。跨 JVM 测试 1 项通过，子节点正常退出；跨容器部署演练仍待完成。
## 2026-09-29：迁移 RPC 服务端幂等合并

`ZfooPlayerMigrationHandler` 接入 `RpcIdempotencyRegistry`，相同 commandId 的重试会共享原始操作结果，冻结、提交等迁移阶段不会因网络重试重复执行。新增重复命令测试，transport-zfoo 全模块 73 项通过。

## 2026-09-29：结构化运行时日志接入

新增 `RuntimeEventLogger`，提供有界事件字段、JSON 输出、敏感字段脱敏和 sink 异常隔离；`ServiceNodeController` 已记录 active、re_registered、draining、failed、stopped 生命周期事件。新增 2 项日志器测试和 1 项生命周期接线测试，runtime 共 82 项通过。

## 2026-09-29：双节点 TCP 玩家迁移演练

新增 `PlayerMigrationTcpExerciseTest`：启动两个独立监听的 zfoo TCP Game 节点，客户端通过真实 socket 发送 RPC envelope，服务端解码迁移命令并回包，协调器完成冻结、准备、释放、提交，最后才发布新路由。1 项端到端测试通过；跨 JVM 本机演练已在后续测试中通过，跨容器部署演练仍待执行。

## 2026-09-28：JDK 25 发布镜像模板

新增 `deploy/Dockerfile` 多阶段构建：使用 Maven/JDK 25 编译、JRE 25 运行，最终以非 root 用户启动 `game-demo --serve`；`deploy/README.md` 补充镜像构建、端口和 Secret 注入说明。

## 2026-09-28：统一运行时运维快照

新增 `RuntimeOperations`，将 `HealthRegistry`、`RuntimeMetrics` 和 `GracefulDrain` 的状态组合成一次性快照，并输出带排空状态和 in-flight 数量的 Prometheus 文本。新增 2 项 `RuntimeOperationsTest`，runtime 共 79 项测试通过；`deploy/compose.yaml` 同步加入 MongoDB、Redis、MySQL 健康检查和本地开发说明。

## 2026-09-28：zfoo 玩家迁移节点协议接线

新增 `PlayerMigrationCommand`/`PlayerMigrationReply`、`ZfooPlayerMigrationNode` 和 `ZfooPlayerMigrationHandler`。客户端为冻结、释放、恢复、准备、提交、取消生成稳定幂等 commandId，复用 `ZfooRpcClientAdapter` 的 deadline、有限重试和重复响应过滤；服务端通过现有 `ZfooRpcServerHandler` 分流并返回 typed reply。`ZfooPlayerMigrationNodeTest` 2 项通过，transport-zfoo 全模块 72 项通过。

## 2026-09-28：P6 外部密钥注入基础

新增 `SecretResolver`，支持注入 Map、环境变量前缀和系统属性回退，缺失密钥只报告名称；`requiredChars` 返回防修改字符副本。新增 3 项 `SecretResolverTest`，runtime 共 77 项测试全部通过。

## 2026-09-28：玩家 Actor 跨进程迁移业务门面

新增 `CrossProcessPlayerMigration`，把玩家快照、源节点冻结/释放/恢复、目标节点准备/提交/取消和路由 owner 发布接入 `CrossProcessMigrationCoordinator`；路由只在目标提交完成后发布，路由发布失败保留 `RECOVERY_REQUIRED` 且不自动恢复源节点。新增 2 项 `CrossProcessPlayerMigrationTest`，game-scene 共 35 项测试全部通过。

## 2026-09-28：全仓库单元回归

执行 `mvn -B -ntp test`，16 个模块全部成功，243 项单元测试通过，0 失败、0 错误、0 跳过。覆盖迁移日志接入后的 runtime、Redis/MySQL、zfoo 网络、场景和 demo 回归。
## 2026-09-28：P5 失联重注册与持久化迁移日志

`ClusterScalingDemo` 现在覆盖两个 Game 节点扩容、固定路由、共享租约心跳失败、递增 generation 重新注册、旧代际 fencing、恢复后路由、排空和缩容；`ClusterScalingDemoTest` 与 game-demo 全模块 12 项测试通过。新增 `MigrationJournal` 接口及 Redis/MySQL 适配器：Redis 使用原子 CAS/条件删除和 TTL，MySQL 使用显式 SQL、版本条件更新和恢复态索引；`CrossProcessMigrationCoordinator` 已接入可选日志生命周期，`MigrationJournalTest` 验证成功清理、恢复态保留和陈旧恢复工作者 fencing。Docker MySQL 真实测试 2 项、隔离 Redis 真实测试 6 项全部通过，0 失败；临时 Redis 容器已删除。
## 2026-09-27：ServiceNodeController 失败重注册

新增失败节点重新注册路径：心跳或共享租约失败后，节点可以用递增 generation 获取新租约，旧代际被 fencing；重新注册成功后清除 draining、恢复 ACTIVE 状态并重新启动心跳调度。`ServiceNodeControllerTest` 4 项全部通过，覆盖租约失败、重新注册、代际递增、路由可见状态和最终排空。

## 2026-09-27：P3.5 全量故障矩阵验收

在隔离 MongoDB 8.0（27018）、MySQL 8.4（3308）和 Redis 7.4（6381）容器上完成剩余故障验证；现有开发环境服务未被停止。协议故障代理 9 项、积压边界 3 项、两轮停止/恢复 3 项、三后端资源压力 2 项全部通过，共 17 项通过、0 失败、0 跳过。测试覆盖确认丢失、服务端延迟响应、幂等重试、队列上限、多 lane、两轮重启后的新连接读写和三后端同时积压。期间修正了 MySQL 健康检查密码参数（`-p`）以及 Mongo 驱动重试后已确认写入的测试断言；临时容器已删除。

## 2026-09-27：跨数据库协议故障代理验证

恢复 Docker Redis 7.4 测试容器（6380），使用本机 MongoDB 27017、Docker MySQL 3307 和 Redis 6380 执行 `game-storage-fault-test`。修正测试启动脚本中 PowerShell 对 `` 的变量解析问题后，`ProtocolFaultMatrixIT` 6 项和 `LateResponseRetryMatrixIT` 3 项全部通过，0 失败、0 错误。验证了 Mongo/MySQL/Redis 的确认丢失、服务端响应延迟、UNKNOWN 结果、独立查询确认副作用，以及相同操作 ID 的迟到响应和幂等重试。Mongo 延迟写入允许重试后返回已确认结果，同时保留独立落库检查。`BacklogMatrixIT` 3 项、`RecoveryCycleMatrixIT` 3 项、`ResourcePressureMatrixIT` 2 项需要三套隔离容器的 stop/start 编排，本轮仍跳过。

## 2026-09-27：MongoDB 实机与故障恢复验证

本机 Windows MongoDB 服务监听 `127.0.0.1:27017`，普通连接验证已通过。随后使用本地已有的 `mongo:8.0` 镜像启动隔离容器 `gameframe-p35-mongo-20260920`（宿主机端口 `27018`），设置 `GAME_TEST_MONGO_URI`、`GAME_TEST_P35_MONGO_URI` 和容器名后执行 `mvn -pl game-storage-mongo -am -B -ntp verify`：`game-storage-core` 28 项单元测试、Mongo 普通集成测试 9 项、Mongo 停止/重启故障矩阵 2 项全部通过，0 失败、0 跳过。验证覆盖脏文档集合、组件拆分、字段级更新、全量保存、批量查询、服务停止时有界队列、恢复后新 store 读取和已提交文档持久性。测试结束后已删除临时容器。

## 2026-09-26：Docker Redis/MySQL 集成复验

Docker 已恢复：Redis 7.4 测试容器使用 6380，MySQL 8.4 使用 3307。执行全量 `mvn -B -ntp verify`，16 个模块成功；RedisStoreIT 5 项、RedisServiceDirectoryMultiProcessIT 2 项、MySqlStoreIT 1 项、MySqlServiceLeaseRegistryIT 1 项、MySqlServiceDirectoryMultiProcessIT 2 项全部实际通过。全量共 279 项测试，246 项通过、33 项因 Mongo/代理等环境未配置而跳过，0 失败、0 错误。

## 2026-09-26：跨进程迁移状态机

新增 `CrossProcessMigrationCoordinator`，覆盖冻结、目标准备、源释放、目标提交、取消、超时和恢复确认；释放/提交阶段失败会保留恢复态，准备请求仍在途时不会擅自恢复源节点。同模块新增 4 项状态机测试，全部通过。随后执行全量 `mvn -B -ntp verify`：16 个模块成功，279 项测试中 235 项通过、44 项因外部环境未配置而跳过，0 失败、0 错误。

## 2026-09-26：可靠 UDP 远端重连隔离

ReliableUdpChannelHandler 现在在远端会话代际变化且序号重新从 1 开始时重置接收窗口，清理旧代际的未确认发送窗口，生成新的本地发送代际，并通过 PeerReconnected 用户事件报告被放弃的片段数量；迟到旧代际数据包和 ACK 会被拒绝。新增 2 项回归测试，可靠 UDP handler 测试共 7 项通过。首次 HELLO 可先于 DATA 建立对端状态；真实 UDP demo 会跳过 HELLO 控制帧。JDK 25 下全量 `mvn -B -ntp verify` 的 16 个模块成功，275 项测试中 231 项通过、44 项因外部环境未配置而跳过，0 失败、0 错误。本次 Docker 引擎未运行，因此 Redis/MySQL 容器集成测试未复验。

## 2026-09-25：RPC 连接池基础

新增 `RpcConnectionPool`，可作为 `GameRpcClient.Transport` 使用；按目标地址维护有界连接集合和单连接 in-flight 上限，连接异常会被摘除，后续调用通过工厂自动建立新连接。新增 3 项测试覆盖故障重连、并发拒绝和连接扩展。

## 2026-09-25：Redis/MySQL 双节点并发压力验证

新增 `RedisServiceDirectoryMultiProcessIT.remainsConsistentUnderConcurrentMetadataPressure` 和 `MySqlServiceDirectoryMultiProcessIT.remainsConsistentUnderConcurrentMetadataPressure`。两个测试分别使用三个独立后端客户端，让两个节点并发高频发布 load，读取端同时轮询目录，验证无重复实例、无负载越界和静默更新丢失；Redis 与 Docker MySQL 各实测通过 1 项。

## 2026-09-25：MySQL 双节点跨进程控制面演练

新增 `MySqlServiceDirectoryMultiProcessIT`，使用三个独立 MySQL 客户端模拟两个 Game 节点和一个目录读取端，验证租约注册、负载上报、draining 传播和释放后的远端路由视图删除。Docker MySQL 实测通过 1 项，MySQL 模块完整 verify 通过。
## 2026-09-25：Redis 双节点跨进程控制面演练

新增 `RedisServiceDirectoryMultiProcessIT`，使用两个独立 Redis 客户端模拟两个 Game 节点，验证共享租约注册、负载上报、draining 状态传播，以及节点释放后远端路由视图删除。Docker Redis 7.4 实测通过 1 项；Redis 模块完整 verify 中普通存储 5 项、故障矩阵 3 项和双节点演练 1 项均通过。
## 2026-09-25：Docker Redis 真实集成验证

创建独立容器 `gameframe-redis-it`（Redis 7.4，宿主机 6380），运行 `RedisStoreIT` 的 5 项测试和 `RedisFaultMatrixIT` 的 3 项测试全部通过，覆盖 TTL、token 条件释放、幂等 claim/complete、过期接管、结果重放、韧性适配器、Redis 停止期间在途请求失败、重启恢复和有界队列拒绝。与 MySQL 集成结果合并后，当前报告汇总为 262 项、0 失败、0 错误、30 项跳过。
## 2026-09-25：Docker MySQL 租约元数据实测

Docker 已恢复并检测到 `orbais-mysql`（MySQL 8.4，宿主机 3307）。使用容器内部配置的账号运行 `MySqlServiceLeaseRegistryIT` 和 `MySqlStoreIT`，2 项真实集成测试通过；租约表创建、旧表元数据迁移、load/draining 更新、续租、代际替换、旧 token fencing、列表读取和释放均通过。`MySqlFaultMatrixIT` 的 2 项故障代理测试因未配置代理环境而跳过。包含此次集成测试的报告汇总为 262 项、0 失败、0 错误、38 项跳过。
## 2026-09-25：P5.3 服务目录租约元数据跨进程发布

`ServiceNodeController` 现在把负载和 draining 状态同时写入共享租约；Redis 记录兼容旧 5 段格式并发布 7 段新格式，MySQL 自动补齐 `load_count` 和 `draining_flag` 列并支持旧表迁移。Redis/MySQL Bridge 会把这些字段转换为远端路由视图，新增服务节点元数据回归测试 1 项、Redis Bridge 元数据测试 1 项；全量测试为 260 项通过、0 失败、0 错误，40 项外部数据库测试跳过。
## 2026-09-25：P5.3 Redis/MySQL 服务目录自动桥接

Redis watcher 现在每次成功轮询都会刷新远端租约有效期；新增 RedisServiceDirectoryBridge 和 MySqlServiceDirectoryBridge，完整快照才执行缺失实例删除，达到 maxEntries 的可能截断快照只更新不删除；新增 4 项测试通过。全量测试总数更新为 260。


## 2026-09-25：P5.3 Gate/Game 扩缩容控制面样例

新增 ServiceDirectorySnapshotTracker，完整快照可安全移除消失实例；新增 game-demo ClusterScalingDemo，验证两个 Game 节点扩容、Gate 路由、节点排空、快照移除和缩容后切换；新增 3 项测试通过。全量测试总数更新为 254。

## 2026-09-24：P5.3 远端目录快照应用层

新增 ServiceDirectorySnapshotApplier，并为 Redis/MySQL PublishedLease 提供 Observation 转换，统一处理远端快照的 ADDED、UPDATED、FENCED、EXPIRED 和容量结果；新增 2 项测试通过。全量测试总数更新为 251。

## 2026-09-24：P5.3 服务节点生命周期控制器

新增 ServiceNodeController 和 Redis/MySQL LeaseStore 适配入口，统一本地注册、共享租约获取/续期、负载上报、优雅排空、拒绝新工作和租约释放；新增 3 项测试通过。全量测试总数更新为 249。

## 2026-09-24：P6 运行时配置与指标基础

新增 GameServerConfig，提供默认值、属性解析和端口/连接数/tick/deadline 范围校验；新增 RuntimeMetrics，提供有界并发计数器、仪表、稳定快照和 Prometheus 文本导出；新增 3 项测试通过。全量测试总数更新为 246。

## 2026-09-24：P5.3 ServiceDirectory RPC 路由

新增 ServiceDirectoryRpcRouter：每次重试重新从远端目录选择实例，失败实例按冷却窗口排除，保持同一 commandId、payload 和 deadline；新增路由测试 3 项通过。全量测试总数更新为 243。

# 验证记录

## 2026-09-24：P5.3 zfoo 客户端 RPC 适配器

新增 ZfooRpcClientAdapter，基于 Session 和可选 UDP attachment 发送 RpcRequestEnvelope，按 commandId 接收并解码 typed response，重试复用同一个请求和 deadline。新增客户端适配器测试 1 项通过。全量测试总数更新为 243。


## 2026-09-24：P5.3 远端服务目录合并视图

新增 ServiceDirectoryView 和统一 Observation，支持 Redis/MySQL 快照合并、代际与 token fencing、墓碑、过期清理、容量限制和按能力/权重/负载选择；新增 3 项 ServiceDirectoryViewTest，通过。

## 2026-09-24：P6 健康检查注册表

新增 HealthRegistry，支持有界异步检查、稳定排序、单项超时隔离、异常降级为 UNKNOWN，以及 UP/DEGRADED/DOWN 聚合；新增 3 项 HealthRegistryTest，通过。

## 2026-09-24：P6 优雅排空协调器

新增 GracefulDrain，统一请求准入 permit、排空状态切换、超时报告、逆序资源关闭和关闭异常收集；新增 3 项 GracefulDrainTest，通过。

## 2026-09-24：Docker MySQL 实测

检测到 `orbais-mysql`（MySQL 8.4，宿主机 3307）。使用容器账号运行 `MySqlStoreIT` 和新增 `MySqlServiceLeaseRegistryIT`，共 2 项实际执行通过；覆盖事务回滚、建表、租约发布、续租、同代重复占用、代际替换、旧 token fencing、列表读取和释放。当前 Docker 未运行 Redis，因此 Redis 集成测试仍保持跳过。

## 2026-09-24：P5.3 Redis 变更监听与 MySQL 租约适配

新增 RedisServiceDirectoryWatcher，以有界 SCAN 快照和变更指纹触发服务目录回调；RedisStore.scanKeys 改为遍历多页游标直到达到上限。新增 MySqlServiceLeaseRegistry，使用显式 SQL、SELECT FOR UPDATE、代际和 token 条件完成初始化、发布、续租、释放和有界列表读取。两个存储模块均已通过 JDK 25 编译验证，外部 Redis/MySQL 集成测试仍按环境配置执行。

## 2026-09-24：P5.3 Redis 跨进程服务租约适配器

新增 RedisServiceLeaseRegistry 和 RedisStore.renewLease/scanKeys，服务实例发布、续租、释放和有界快照发现都使用 TTL 与 token 条件脚本；旧实例无法覆盖或删除新实例。当前适配器已完成编译验证，Redis 集成测试仍需配置可用 Redis 服务。


## 2026-09-24：P5.3 zfoo RPC envelope 与服务端分流

新增 RpcRequestEnvelope、RpcResponseEnvelope、ZfooRpcCodec、ZfooRpcPendingResponses 和 ZfooRpcServerHandler；TCP、WebSocket、HTTP server 支持可选 RPC handler，响应按 commandId 关联并拒绝迟到/重复响应。新增协议与 handler 测试 4 项通过。全量测试总数更新为 243。


## 2026-09-24：P5.3 zfoo RPC 传输门面

新增 GameRpcClient，将 RpcCallExecutor 接到 game-transport-zfoo 的传输边界；TCP、WebSocket、UDP 适配器只需实现 Transport.send，重试复用同一个 commandId 和 deadline。新增 GameRpcClientTest 3 项通过。全量测试总数更新为 243。



## 2026-09-24：P5.3 RPC 调用执行器

新增 RpcCallExecutor，统一处理总 deadline、单次超时、有限重试、按目标服务熔断、半开探测和关闭清理。重试调用沿用稳定 command ID，可与 RpcIdempotencyRegistry 组合使用。新增 RpcCallExecutorTest 5 项通过。全量测试总数更新为 243。



## 2026-09-24：P5.3 RPC 幂等请求合并

新增 RpcIdempotencyRegistry，按稳定 command ID 合并并发重试，只执行一次原始操作，并在有限保留窗口内复用完成结果。新增 RpcIdempotencyRegistryTest 3 项通过。本次新增 3 项，累计全量测试总数为 243。



## 2026-09-23：P5.3 RPC pending call 基础

新增 RpcPendingCalls，提供有界请求登记、绝对 deadline、控制面批量过期、关闭时失败，以及完成后重复响应和未知响应过滤。RpcPendingCallsTest 4 项通过。有限重试与熔断继续复用已有存储韧性执行器，统一 RPC 适配器已在后续阶段完成。



## 2026-09-23：P5.2 租约 fencing 与地图路由撤销

在 ServiceDirectory 上增加代际注册、随机租约令牌、过期墓碑、运维排空保持和路由 fencing。旧代际的心跳、负载上报、排空操作和路由重新绑定都会被拒绝；实例注销、租约过期和排空会撤销地图路由。新增 ServiceDirectoryLeaseTest 5 项通过。该实现仍是进程内控制面基础，未声称跨进程一致性。


## 2026-09-23：P5.1 服务目录基础

新增 `ServiceDirectory`，支持角色与能力标签、实例容量、有界租约、心跳续租、过期剔除、负载上报、排空标记和确定性负载感知选择。`ServiceDirectoryTest` 5 项通过。当前目录是进程内控制面基础，尚未连接跨进程 Registry/RPC。


## 2026-09-23：P4.7 真实多传输认证契约

新增 `AuthenticationRequest`/`AuthenticationResponse` zfoo 协议包和 `DemoZzzAuthenticationNetworkTest`。测试启动真实 `GameTcpServer`、`GameWebsocketServer`、`GameUdpServer`，使用 JDK Socket、WebSocket、DatagramSocket 客户端验证错误密钥不消耗 nonce、正确密钥完成 uid Actor 晋级、重复 nonce 返回 `REPLAY`。
## 2026-09-23：P4.7 认证边界基础

新增 ReplayGuard，按身份和 nonce 建立有界去重集合，校验过去时间窗与未来偏差，自动清理 TTL 到期记录；容量达到上限时拒绝新 nonce。ReplayGuardTest 5 项通过，覆盖身份作用域、时钟边界、过期复用和容量攻击。

新增 GameAuthenticationService，通过账号查找、常量时间密钥比较、ReplayGuard 校验和 GameSessionBinder 完成认证到 uid Actor 的异步晋级。GameAuthenticationServiceTest 3 项通过，覆盖错误密钥、重复 nonce 和重复 uid 登录。认证协议包和三种传输端到端网络契约已接入；账号查询通过外部函数注入，后续只需接入实际账户服务。

## 2026-09-23：P4.6 TLS/mTLS/WSS

GameTlsContext 支持服务端 TLS、客户端信任、mTLS 和主机名校验。GameTlsContextTest 覆盖 TCP/WebSocket 管线位置、TLS 1.3、错误主机名、未信任证书和 mTLS 客户端证书。DemoTlsWebsocketNetworkTest 使用真实 JDK WebSocket 客户端完成 WSS 握手。

## 全量验证

执行 `mvn -B -ntp clean verify`，16 个模块全部成功；共 260 个测试，0 失败、0 错误，40 个外部数据库测试因未配置环境而跳过。`git diff --check` 仅报告现有 `.gitignore` 换行符提示。

## 已验收的网络契约

TCP、UDP、HTTP、WebSocket 均有真实客户端测试；可靠 UDP 覆盖 ACK、有限重传、乱序、去重、分片、CRC32C、序号回绕和会话代际。UDP 逻辑 Session 按远端地址隔离并执行容量和空闲回收。