# 游戏对象与场景同步（P1）

运行样例（JDK 25）：

```powershell
mvn -B -ntp verify
java -jar game-demo/target/game-demo-0.1.0-SNAPSHOT.jar --scene
```

样例在一个地图 Actor 内创建移动组件和被动背包组件，执行四个逻辑 tick、两次网络刷新模拟；没有开启监听端口或访问数据库。
前三级移动只保留最终位置；两个观察者共享一次编码。一个观察者离开后，后续刷新只发送给仍然可见的观察者。
完整代码在 game-demo/src/main/java/io/gameframe/demo/SceneDemo.java，独立 JVM 打包测试验证它可运行和退出。

## GameObject / GameComponent

game-scene 依赖 game-runtime。GameObject、组件和 ActiveComponentScheduler 由同一 Actor 独占。
不能从数据库线程、网络线程或其他 Actor 修改它们；通过 ActorRef.tell/call/pipe 返回所属 Actor。Actor 可能换工作线程，校验的是 Actor 身份。

组件类型沿用 ServerFrame 的 16 位约定：高 8 位是大类槽（1..255），低 8 位是子类型。
同一 GameObject 的同一大类只能存在一个组件，例如 0x0100 玩家移动与 0x0101 坐骑移动不能同时占用移动槽。

```java
class Movement extends GameComponent {
    Movement() {
        super(0x0100);
        updatePolicy(UpdatePolicy.everyFrame());
    }
    @Override protected void onUpdate(long elapsedNanos) {
        // 已在所属 Actor 内；owner().id() 是实体 ID。
    }
}
```

- add 调用 onAttach，成功后注册活跃调度。挂载失败或活跃容量不足会移除槽位，并调用 onDetach 清理部分初始化。
- onAttach 必须与 onDetach 配对设计；部分初始化失败时 onDetach 也可能执行。
- remove 先移除调度，再调用 onDetach；异常不会留下可再次调度的组件。
- close 清理所有组件并汇总异常，重复 close 幂等。必须在所属 Actor 内关闭，再关闭 ActorSystem。
- onAttach/onDetach 中禁止递归增删当前对象的组件。onUpdate 中允许添加、移除自己或其他组件，新加入组件从下一次 update 开始执行。
- 未挂载组件由创建者独占，可先配置策略；挂载后策略访问也检查 Actor。不要让网络线程持有并操作组件引用。
- GameObject 只管理组件，不自动管理 AOI、数据库或实体仓库；实体退出时要先从 AOI 移除并清理移动状态，再关闭对象。

## 活跃组件调度

一个地图/房间共享一个 ActiveComponentScheduler(actor, maxActive, maxUpdatesPerTick)。
GameObject 添加、移除或变更策略时自动注册/注销，业务层不再对每个对象全量遍历组件。

| 策略 | 行为 |
|---|---|
| passive() | 默认策略，不占活跃集合、不会每帧遍历 |
| everyFrame() | 每次调度尝试执行一次 |
| everyTicks(n) | 累计 n 次调度后执行 |
| interval(Duration) | 累计模拟时间到间隔后执行 |

update(elapsedNanos) 返回 active/examined/executed/deferred/failures。
预算不足时保留累计时间；已执行组件轮转到队尾，避免饿死。慢帧每组件最多回调一次，传入实际累计模拟时长，不创建补帧任务风暴。
切换策略会清空该组件旧的累计时间；容量不足时拒绝激活，保留原策略。

组件 onUpdate 抛 RuntimeException 时会暂停其主动更新，其他组件继续；Stats.failures 带实体、类型和异常。
业务必须处理这个结果，决定告警、修复或重建。它不是自动 Actor 监督重启；Error 仍向外传播。
maxUpdates 限制回调数量，不能抢占慢回调；每次最多检查 maxActive 个组件，仍需按地图容量配置预算。

## AOI 变化

GridAoi 原有 move/remove/visible 兼容保留。新业务使用带 Actor 和 maxEntities 的构造方法。
新增 moveWithChanges/removeWithChanges 返回不可变 VisibilityChange 列表，每次关系变化有观察者→目标和目标→观察者两个事件。
先完成索引更新再返回事件，业务发送通知失败不会破坏索引。无效坐标或新增容量拒绝发生在修改前。
同一格移动不产生通知；跨格只报告集合差；负坐标按 floor 计算。

查询半径是格子数，非几何距离。尚未提供私有对象过滤、初始生成快照、事件序号、ACK 和断线补齐。

## MovementReplicator

markDirty(entityId, payload) 使用不可变拷贝，返回 ACCEPTED/REPLACED/UNKNOWN_ENTITY/CAPACITY/TOO_LARGE。
Limits 限制脏实体数、脏载荷总字节、单实体字节、单包字节和单周期观察关系数。
使用有实体容量限制的 GridAoi；每次编码缓存字节不超过 maxPendingBytes。
这些是该复制器的预算，不包含业务编码器任意分配的内存、Netty 发送队列或整进程总内存。

flush 按当前可见性组织批次。同一实体集合在本周期内共享编码，Encoder 不接收观察者 ID，防止误把带观察者私有信息的编码跨观察者复用。
State 的字节访问和给 Sender 的包都使用防御拷贝。Sender 返回 true 仅表示接受发送，不代表客户端收到。

刷新会消费当前批次；拒绝和失败通过 FlushStats 返回，超过观察关系预算时整批放弃并设置 visibilityBudgetExceeded。
不会无限保存旧移动包。Encoder/Sender 回调中新标记的数据保留到下一批；不允许递归 flush。
实体离开时先 removeWithChanges，再 forget；进入可见范围的完整出生状态由业务处理可见性事件。

必须发送可丢弃的绝对移动状态，并周期性发布最新状态。如果只在首次变脏时发送且失败后不再标记，客户端可能停留在旧位置。
这里未实现传输队列中的包覆盖、可靠增量基线、分片或重连修复；这些在 TODO P4。
不得用于金币奖励、背包修改、交易或其他不可丢弃业务。

## 基础工具复核（P0）

ObjectRegistry 的快照是不可变的浅快照。遍历条目不能改映射，但值对象的字段仍需自己的 Actor/线程规则。
emplace 工厂在锁外执行：竞争者可以各建一个候选，最终只注册一个；所有竞争调用返回已注册的获胜对象。工厂不要执行不可重复的外部副作用。

ObjectPool 的容量是闲置缓存上限，不是活跃租借数量上限。Lease 必须通过 try-with-resources 显式 close；Java GC 不会代为归还。
reset 抛异常的对象不会回池；重复 close 不重复归还。Lease 与借出的对象由借用者独占，归还后不得保留或使用旧引用。
这是可选基础工具，尚未有性能数据证明具体业务对象应该池化。
