# 本地验证指南

仅依赖 JDK 21、Maven 与工程自带的 Spring Boot 依赖；不需要任何外部服务或凭据。

## 1. 运行全部单元测试

```bash
mvn test
```

16 个用例覆盖以下场景（测试包
`com.github.lisama569.durablefunctionruntimejava.engine.tests`）：

| 测试类 | 覆盖点 |
| --- | --- |
| `SequentialExecutionTest` | 顺序推进、输入/输出持久化、事件流水打印 |
| `RetryAndFailureTest` | 可重试错误按指数退避重试后成功、不可重试错误立即失败且 code 稳定、重试耗尽落入终态、终态不回退、副作用仅一次 |
| `RestartRecoveryTest` | 完成态重启不重放、崩溃遗留 `RUNNING` 恢复为 `INTERRUPTED` 并重试、退避态重启后继续遵守截止时间 |
| `AsyncCallbackTest` | 异步等待→回调完成、重复/乱序/未知回调忽略、失败回调换新 token 重试直至终态、异步超时重试 |
| `DuplicateSubmissionAndTerminationTest` | 重复提交不重跑、终止进入 `TERMINATED` 且后续驱动/终止/启动均不能回退 |
| `ConcurrencyTest` | 24 个实例并发同步执行互不串扰；12 个实例 × 5 重复回调并发风暴下完成计数恒为 1/实例 |

- 失败原因可区分：断言 `errorType`（`NON_RETRYABLE` / `RETRYABLE` /
  `TIMEOUT` / `INTERRUPTED` / `RETRIES_EXHAUSTED` / `TERMINATED`）与稳定
  `errorCode`（如 `UPSTREAM_BUSY`、`BAD_ORDER`、`STEP_TIMEOUT`）。
- 幂等断言：`SideEffectCounters` 统计真实副作用执行次数，所有重试 / 重放 /
  重复回调后每个 `(实例,步骤)` 恒为 1。
- 日志：测试日志中逐条打印 `实例 / 步骤 / 尝试号 / 事件 / 详情`，
  可用以下命令查看：

```bash
mvn -Dtest=RetryAndFailureTest test | grep DURABLE
```

## 2. 启动本地 HTTP 服务并手工验证

```bash
mvn spring-boot:run
# 默认端口 8080，可用 --server.port=8099 覆盖
```

接口（base `/workflows`）：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /workflows/start` | 启动 / 幂等重复提交实例 |
| `GET  /workflows` | 列出全部实例 id |
| `GET  /workflows/{id}` | 查看完整聚合（步骤、尝试、事件流水） |
| `POST /workflows/{id}/terminate?reason=` | 终止 |
| `POST /workflows/{id}/complete` | 异步步骤完成回调（携带 token） |
| `POST /workflows/demo/simulate-failures/{id}?count=N` | 演示：注入 N 次瞬时失败 |

### 2.1 顺序执行 + 重试 + 重复提交（内置 `demo-order` 工作流）

```bash
# 让 order-777 的计费步骤先失败 2 次，第 3 次成功
curl -s -X POST 'http://localhost:8080/workflows/demo/simulate-failures/order-777?count=2'
curl -s -X POST http://localhost:8080/workflows/start \
  -H 'Content-Type: application/json' \
  -d '{"workflowName":"demo-order","instanceId":"order-777","input":{"orderId":"order-777"}}'

# 等待退避（200ms 起，指数翻倍）后观察：status=COMPLETED，charge.currentAttempt=3
curl -s http://localhost:8080/workflows/order-777 | jq \
  '{status, attempt: .steps.charge.currentAttempt, output, events: [.events[].type]}'

# 重复提交：状态不变，事件中新增 START_DEDUPED，不产生新尝试
curl -s -X POST http://localhost:8080/workflows/start \
  -H 'Content-Type: application/json' \
  -d '{"workflowName":"demo-order","instanceId":"order-777","input":{"orderId":"order-777"}}'
```

### 2.2 不可重试错误立即终态

```bash
curl -s -X POST http://localhost:8080/workflows/start \
  -H 'Content-Type: application/json' \
  -d '{"workflowName":"demo-order","instanceId":"bad-1","input":{}}'
# status=FAILED, errorType=NON_RETRYABLE, errorCode=DEMO_BAD_ORDER, validate.currentAttempt=1
```

### 2.3 终止语义

```bash
curl -s -X POST 'http://localhost:8080/workflows/demo/simulate-failures/term-1?count=99' >/dev/null
curl -s -X POST http://localhost:8080/workflows/start -H 'Content-Type: application/json' \
  -d '{"workflowName":"demo-order","instanceId":"term-1","input":{"orderId":"term-1"}}' >/dev/null
curl -s -X POST 'http://localhost:8080/workflows/term-1/terminate?reason=manual%20cancel'
# status=TERMINATED；再次 terminate 只追加 TERMINATE_IGNORED
```

## 3. 检查本地持久化文件

```bash
ls target/durable-state/instances/
cat target/durable-state/instances/order-777.json | jq '.steps.charge'
ls target/durable-state/effects/order-777/charge/   # 幂等副作用结果文件
```

重启进程后再次 `GET /workflows/order-777`，状态与事件流水完整保留；
后台 ticker 启动时自动扫描并恢复所有实例。

## 4. 配置（`src/main/resources/application.properties`）

```properties
durable.engine.storage-dir=target/durable-state  # 本地状态目录
durable.engine.tick-interval-ms=200              # 超时/退避扫描周期
durable.engine.auto-start=true                   # 是否启动后台恢复与 ticker
```
