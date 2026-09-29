# ServerFrame 特性移植计划

执行入口：[根 TODO 清单](../TODO.md)。该清单是唯一进度来源；本文解释取舍和实施顺序。

## 目标和判断方式

在本地 zfoo 源码基础上建设 JDK25 游戏框架，迁移 ServerFrame 中更适合游戏场景的行为。
“更好”按具体能力和可验证边界判断，不代表 C++ 的全部实现都比 zfoo 快；尚未进行两框架的公平性能基准。

基线：ServerFrame d445920a7b510785bf12cc2e324b5a19d9f8bc89，zfoo e426a7ebb3070f32b774d97d6cf58c09c21a6817。
参考源码保持只读，Java 实现在当前项目中维护。

## 能力对照

| 能力 | ServerFrame 依据 | zfoo/当前框架现状 | 处理方式 |
|---|---|---|---|
| 工厂、对象身份管理、短命对象复用 | framework/base/Factory.h、ObjectManager.h、ObjectPool.h | game-base 初版已加入 | P0 复核并发和失败边界；对象池按需使用 |
| 组件生命周期和活跃更新策略 | framework/gameobject | zfoo 不提供同等游戏对象层 | P1 新建 game-scene，Actor 限定状态访问 |
| AOI 可见性变化和移动批合并 | framework/game/AoiGrid.*、MovementReplicator.* | 已有双向可见性差分 | P1 补双向差量和有界批同步 |
| 对象事件与组件类型工厂 | base/EventCenter.h、EventEmitter.h、gameobject/ComponentFactory.h | zfoo event 和 game-base Factory 可复用 | P2 补作用域注销与组件配置创建，不照搬未完成的取消监听 |
| Actor 监督、共享定时、Zone 迁移 | framework/actor、game/ZoneTransferCoordinator.* | 已有 mailbox/公平调度/pipe、生命周期、共享定时和进程内迁移 | P2 补生命周期、共享定时和可验证迁移状态机 |
| 多数据库显式接口 | framework/database | game-storage-* 已有基础接口 | 保留不依赖 ORM 的设计；P3 增加批量/脏合并/幂等 |
| TCP、UDP、WS、WSS、HTTP | framework/network | zfoo net/core 已有这些服务端；有 Session 和 codec | P4 复用实现，补游戏入口和验证，不重复造监听器 |
| 防重放与身份限流 | base/security/AntiReplay.*、SecurityUtils.* | 尚无游戏认证入口 | P4 补有界 nonce、身份/时间窗及会话代际 |
| 可靠 UDP、大包重组 | network/udp/UDPSession.* | zfoo 普通 UDP 不等于可靠有序通道 | P4 独立有界可靠层和故障注入 |
| 最新状态覆盖和发送预算 | network/core/OutboundBudget、tcp/TCPSession、websocket | ZfooSender 已有部分预算 | P4 补覆盖、统一统计和入站限流 |
| 动态发现、排空、负载路由、RPC 治理 | framework/cluster、base/rpc | zfoo 有 Registry/Consumer、负载均衡和 asyncAsk | P5 优先在已有机制上适配游戏负载和治理 |
| 配置、日志、监控 | config、log、documents/OPERATIONS.md | 当前只有开发样例/构建脚本 | P6 整合标准 JVM 运维能力 |

## 实施顺序

P0 基础复核 → P1 场景组件与同步 → P2 生命周期与迁移 → P3 存储一致性 → P4 网络增强 → P5 集群与 RPC → P6 运维与配置。
2026-09-20 用户调整：Java 框架不接入 Lua；脚手架暂缓，不属于当前实施范围。保留已有构建/运行脚本。

每一步完成定义：具体 API 与边界文档、有限时且覆盖失败路径的测试、样例或调用点接入、相关 reactor 构建通过。
P1 的验收是进程内场景样例；端到端客户端同步、可靠性/基线/序号和断线恢复属于 P4，不能混称已完成。

## 保留行为，改进边界

- Java Lease 是显式 close，不是 C++ 析构；对象池 reset 抛错时丢弃对象，避免把脏对象重新给业务。
- Registry 工厂在锁外运行，因此竞争时可以创建多个候选，仅一个注册成功；对象字段并不会自动线程安全。
- 组件、AOI 和移动同步归地图/房间 Actor；共享线程池可能切换 OS 线程，应检查 Actor 身份而非固定线程号。
- 组件调度上限要公平轮转，不能让靠前的活跃组件长期挤掉后面的组件。
- 移动批编码复用只能用于与观察者无关的公共状态；私有可见性过滤和观察者专属编码必须隔离。
- Zone 目标暂存与真正激活分开；超时后不可留下源和目标两个活跃实体。
- 所有网络/数据库异步接口需表达拒绝、失败或未知结果；不能把“提交队列”称为“远端已处理”。
- zfoo 自带多语言协议、Netty 事件循环、Spring 集成和注册机制保持复用；C++ 构建和内存管理方式不照搬。

## 验证记录

历史源码导入和数据库联调见 [validation.md](validation.md)。新增阶段验收在同文档按阶段追加，TODO 只引用已经实际运行的结果。
