# GameFrame

GameFrame 是基于 JDK 25 和 vendored zfoo 的游戏服务器框架。项目吸收 ServerFrame 的 Actor、场景、组件存储、局部更新、可靠 UDP 和故障边界设计，同时保留 zfoo 的协议编解码与多协议网络能力。

## 当前状态

P0 至 P5.3 控制面基础已完成首轮：统一 Session/Actor 路由、登录绑定、流量保护、出站预算、TCP/UDP/HTTP/WebSocket 真实契约、可靠 UDP、TLS/mTLS、WSS、认证防重放、进程内服务目录治理，以及有界 RPC 调用执行和 zfoo 传输门面均已验证。跨进程注册中心已接入 Redis/MySQL 租约适配、Redis 有界快照监听、Redis/MySQL ServiceDirectoryBridge 自动快照接线、ServiceDirectoryView 远端目录合并、ServiceDirectoryRpcRouter 重试选路、ServiceNodeController 生命周期与跨进程 load/draining 元数据发布和 zfoo RPC；GracefulDrain、HealthRegistry、RuntimeMetrics、RuntimeOperations、RuntimeEventLogger、GameServerConfig 和 SecretResolver 已接入运行时运维基础；Gate/Game 控制面已加入心跳失效后的递增 generation 重注册和路由恢复演示；已加入 `CrossProcessPlayerMigration` 玩家 Actor 迁移门面，以及 zfoo `ZfooPlayerMigrationNode`/`ZfooPlayerMigrationHandler` 协议接线；双节点 TCP 迁移演练已通过，跨 JVM 本机演练已通过，跨容器部署演练已通过；迁移日志已提供 Redis/MySQL 持久化适配器并接入协调器生命周期；RPC 连接池与可靠 UDP 重连隔离已接入。zfoo 包级 RPC envelope、TCP/WebSocket/UDP 客户端适配、响应关联和 TCP/WebSocket/HTTP 服务端分流已接入。

本轮新增 `CrossProcessMigrationCoordinator`：跨 Game 节点迁移按冻结、准备、释放、提交的顺序执行，并可接入 `MigrationJournal` 持久化阶段和恢复态；释放或提交阶段发生不确定故障会保留 `RECOVERY_REQUIRED`，目标操作未落定时不会自动恢复源节点，且同一 Actor 和迁移 ID 在恢复完成前不会重复迁移。

## 构建

```powershell
mvn -B -ntp clean verify
```

构建要求 JDK 25。当前全量 `mvn -B -ntp test` 为 16 个模块成功、243 项单元测试全部通过；本轮已使用 Docker Redis 6382 隔离容器和 MySQL 3307 完成迁移日志真实集成验证，并在本机 MongoDB 27017 及隔离 Mongo 8.0 容器上完成 11 项真实 Mongo 集成与停止/恢复故障测试；P3.5 跨数据库故障矩阵已完成 17 项真实测试（协议代理、积压边界、停止恢复、资源压力）并全部通过。完整 verify 的外部集成测试仍按环境变量控制执行。

## 存储

业务层使用 `game-storage-core` 的显式接口，Mongo、MySQL、Redis 分别位于独立模块。`DirtyDocumentSet` 支持组件拆分、字段级脏更新和全量保存；数据库访问不要求业务对象绑定 ORM。Redis 模块还提供 `RedisServiceLeaseRegistry` 和 `RedisServiceDirectoryWatcher`，用 token 条件续租、释放、有界快照和变更回调发现服务实例；MySQL 模块提供显式 SQL 的 `MySqlServiceLeaseRegistry`，用行锁完成过期代际替换；`RedisMigrationJournal` 和 `MySqlMigrationJournal` 提供带版本 fencing 的迁移日志、恢复态查询和条件删除。

## 网络与认证

`game-transport-zfoo` 提供 TCP、UDP、HTTP、WebSocket/WSS、可靠 UDP、TLS/mTLS、Session/Actor 路由、认证基础设施和 `GameRpcClient` RPC 传输门面。开发 demo 使用共享玩家接口，没有账户认证和资产流水，不能直接暴露到公网。

开发依赖容器和 JDK 25 发布镜像说明见 [deploy/README.md](deploy/README.md)。详细计划见 [TODO.md](TODO.md)，验证记录见 [docs/validation.md](docs/validation.md)，架构取舍见 [docs/architecture.md](docs/architecture.md)。