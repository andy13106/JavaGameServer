# 本地依赖容器

`compose.yaml` 只用于开发和集成测试，默认绑定到 `127.0.0.1`。启动方式：

```powershell
docker compose -f deploy/compose.yaml up -d
```

服务端口如下：

- MongoDB：`127.0.0.1:37017`
- Redis：`127.0.0.1:36379`
- MySQL：`127.0.0.1:33306`，数据库 `gameframe`，用户 `gameframe`，密码 `gameframe_dev_only`

MySQL、MongoDB 和 Redis 都配置了容器健康检查。生产部署应通过外部 Secret 注入数据库凭据，不要直接复用 compose 中的开发密码；Java 进程可使用 `SecretResolver` 读取注入值、环境变量前缀或系统属性。

停止并删除本地数据卷：

```powershell
docker compose -f deploy/compose.yaml down -v
```
## 构建运行镜像

仓库根目录执行：

```powershell
mvn -B -ntp -DskipTests package
docker build -f deploy/Dockerfile -t gameframe:dev .
docker run --rm --network host `
  -e GAME_MONGO_URI=mongodb://127.0.0.1:37017/gameframe `
  -e GAME_MONGO_DATABASE=gameframe `
  -e GAME_PORT=9000 `
  gameframe:dev
```

`deploy/Dockerfile` 使用已由 JDK 25 Maven 构建的 shaded JAR，再用 JRE 25 构建运行镜像；CI 环境可使用 `deploy/Dockerfile.build` 执行容器内多阶段构建。最终进程以非 root 用户 `gameframe` 启动。容器默认设置 `GAME_BIND_HOST=0.0.0.0`；本机直接运行时默认仍为 `127.0.0.1`。生产环境应把数据库 URI、账号密钥等通过 Secret 或环境变量注入，并按实际网络拓扑替换示例中的 `--network host`。
## 跨容器迁移演练

`MigrationNodeMain` 支持 `--host`、`--name` 和 `--port` 参数。跨容器场景要监听 `0.0.0.0`：

```powershell
docker build -f deploy/Dockerfile.runtime -t gameframe:dev .
docker network create gameframe-migration-it
docker run -i -d --name gameframe-migration-a --network gameframe-migration-it -p 39101:9001 gameframe:dev --migration-node --name=game-a --host=0.0.0.0 --port=9001
docker run -i -d --name gameframe-migration-b --network gameframe-migration-it -p 39102:9001 gameframe:dev --migration-node --name=game-b --host=0.0.0.0 --port=9001
$env:GAME_MIGRATION_SOURCE_PORT='39101'
$env:GAME_MIGRATION_TARGET_PORT='39102'
mvn -pl game-demo -am -B -ntp -Dtest=PlayerMigrationContainerExerciseIT '-Dsurefire.failIfNoSpecifiedTests=false' test
docker rm -f gameframe-migration-a gameframe-migration-b
docker network rm gameframe-migration-it
```

该演练已验证两个独立容器之间的 zfoo 迁移 RPC、阶段幂等和目标提交后的路由发布。
## 性能基线

构建 shaded JAR 后可运行本地基线：

```powershell
java -XX:+UseZGC -jar game-demo/target/game-demo-0.1.0-SNAPSHOT.jar --benchmark --actor-ops=200000 --storage-ops=20000
```

输出 JSON 包含 JDK、操作系统、Actor 邮箱吞吐和内存局部更新吞吐。该入口用于比较同一机器上不同提交的趋势，不能替代生产压测。