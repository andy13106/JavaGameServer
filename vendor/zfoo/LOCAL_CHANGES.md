# 项目内 zfoo 源码

导入日期：2026-09-18。
来源：本机 `D:/zfoo`，上游 https://github.com/zfoo-project/zfoo 。
上游版本：4.1.4；提交：`e426a7ebb3070f32b774d97d6cf58c09c21a6817`。
本项目 Maven 版本：`4.1.4-gameframe-SNAPSHOT`。

## 源码位置与构建

这里是普通源码目录，随 GameFrame 仓库一起维护，不是 Git 子模块，也不依赖本机 `D:/zfoo`。
已导入上游 Git 跟踪的全部 1,333 个文件，包括九个模块、测试、资源和文档；不复制 `.git` 和未跟踪的构建输出。
`LICENSE` 保留 Apache-2.0 许可证；各模块 JAR 和最终游戏 JAR 也包含 `META-INF/zfoo/LICENSE`。

默认构建 `protocol`、`event`、`scheduler`、`net`，由根项目 Maven reactor 统一编译为 Java 25。
`boot`、`hotswap`、`monitor`、`storage`、`orm` 的源码也在此目录，通过 `zfoo-all` profile 加入构建。
游戏框架仍依赖自己的 MongoDB/MySQL/Redis 存储接口，未引入 zfoo ORM 的运行时依赖。

在 GameFrame 项目根目录执行：

```powershell
# 修改 vendor/zfoo/<模块>/src/main/java 后重新构建
mvn -B -ntp verify
# 只构建游戏样例及其依赖（必须保留 -am）
mvn -B -ntp -pl game-demo -am verify
# 将全部九个 zfoo 模块加入编译和打包
mvn -B -ntp -Pzfoo-all package
```

IDE 中导入根 `pom.xml` 并选择 JDK 25。需要修改可选模块时启用 `zfoo-all` profile。
Maven 使用专用版本，避免混用官方 4.1.4 发布包。构建时从根目录使用 reactor，不能仅构建某个游戏子模块而省略 `-am`；否则可能使用本机旧 SNAPSHOT，或因未安装而解析失败。
源码变更在重新构建、重启服务后生效，本次没有实现运行中的自动热更新。

## 本地构建适配

每个 `pom.upstream.xml` 是导入时的原始 POM，仅用于对照，不参与构建。
当前 `pom.xml` 添加 GameFrame 父项目继承和专用版本，统一依赖版本属性及 JDK 25 编译约束。
移除上游签名、Central 自动发布、source/javadoc 附件插件；zfoo 子树禁用 deploy。保留资源打包及 hotswap 的 agent manifest 配置。
`.gitignore` 对上游已跟踪的 `HotswapClass.class` 测试夹具增加例外，确保它能够随仓库保存。
2026-09-20 P2 修改 event/src/main/java/com/zfoo/event/manager/EventBus.java：
- 注册表改为 ConcurrentHashMap + CopyOnWriteArrayList，支持并发发布、注册和注销。
- 新增 unregisterEventReceiver，按接收器身份移除，空类型条目回收；保留原有注册/发布接口。
- 游戏订阅使用 CurrentThread 桥接到有界 Actor 邮箱，不使用 zfoo 无界异步队列。
- 自动回归位于 game-scene 的 ActorEventsTest，包括并发增删发布、身份注销、作用域释放及邮箱拒绝。
- PackagedDemoIT 的 event 代表类改为 EventBus，核对修改后的本地字节码确实进入最终 JAR。

上游测试含独立服务器、压测和外部中间件示例，因此只在 zfoo 子树设置 `maven.test.skip=true`，默认不编译、不执行这些示例测试；这不是通过了上游完整测试套件。
GameFrame 的测试不继承该设置，照常执行。针对后续 zfoo 修改，应在游戏框架中补充有限时、可自动退出的回归测试。
`PackagedDemoIT` 校验四个核心模块的私有版本、Java 25 字节码，以及最终 JAR 的代表类与项目内编译输出逐字节一致；同时测试独立 JVM 启停；真实 TCP 链路由 DemoNetworkTest 验证。

以后直接修改当前目录中的源码，不再回写 `D:/zfoo`。同步上游时以记录的提交为基线审查差异，保留本地构建适配并重新运行协议、TCP、生命周期和存储测试；不要直接覆盖本地修改。