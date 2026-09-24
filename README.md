# 持久化函数工作流执行引擎（单机版）

一套单机运行的持久化工作流引擎：业务逻辑编排为按顺序推进的步骤（Step），
每个实例的步骤输入、输出、尝试次数与状态持久化在**本地文件**中。
不依赖任何外部服务或真实凭据，仅使用 Spring Boot Web 与 Jackson。

## 模块结构

| 位置 | 职责 |
| --- | --- |
| `domain/` | 状态模型：`WorkflowInstance`、`StepRecord`、`WorkflowStatus`、`StepStatus`、`ErrorKind`、`RetryPolicy` |
| `workflow/` | 定义模型：`WorkflowDefinition`、`StepDefinition`、`StepHandler`、`StepContext` |
| `engine/FileWorkflowStore` | 本地 JSON 文件持久化：临时文件 + 原子移动，乐观版本号并发控制 |
| `engine/WorkflowEngine` | 事件驱动执行引擎：单线程事件循环 + worker 线程池 + 定时调度 |
| `web/WorkflowController` | REST 接口：幂等启动 / 查询 / 终止 |
| `web/EngineConfig` | Spring 装配与示例工作流 `orderWorkflow`（validate → charge → ship） |

## 执行模型

1. **启动**：`start(workflowName, idempotencyKey, input)` 创建实例，状态先落盘再投递第一个 `RUN_STEP` 事件。
2. **状态流转唯一**：所有状态迁移都发生在**单线程事件循环**中；业务步骤在 worker 线程池执行，
   完成后投递携带 `(instanceId, stepIndex, attempt)` 的完成通知。
3. **顺序推进**：一个步骤 COMPLETED 并持久化后，才推进 `currentStepIndex` 并调度下一步；
   上一步输出作为下一步输入。
4. **持久化优先**：每次状态迁移先写盘后推进。文件写入采用「临时文件 + 原子 rename」，
   崩溃后不会读到半截文件；更新带 `version` 乐观锁。
5. **完成通知去重**：事件处理前校验「实例 RUNNING + 当前步骤序号匹配 + 步骤 RUNNING + attempt 匹配」，
   任何重复或迟到的通知（重试后旧 attempt 的迟到结果、超时后才返回的结果、终止后到达的结果）
   都会被丢弃，终态不可逆，计数不会错乱。
6. **重启恢复**：启动时扫描所有 `RUNNING` 实例，从 `currentStepIndex` 继续；
   **已 COMPLETED 的步骤不会重复执行**，崩溃时处于 RUNNING 的步骤作为新 attempt 重跑（因此业务副作用需幂等）。

终态：`COMPLETED` / `FAILED` / `TIMED_OUT` / `TERMINATED`，均不可逆。

## 重试、超时与终止规则

- 步骤抛 `RetryableException` → `RETRYABLE`，按指数退避重试（`initialBackoff * 2^(attempt-1)`，封顶 `maxBackoff`）；
- 抛 `NonRetryableException` 或任何未分类异常 → `NON_RETRYABLE`，**立即** `FAILED`，不重试；
- 执行时长超过 `stepTimeoutMillis` → `TIMEOUT`，取消 worker，可重试则退避重试，否则 `TIMED_OUT`；
- 尝试次数硬性上限 `maxAttempts`（含首次）；退避与超时均有明确上界，重试耗尽落入确定终态，不停留中间态；
- `terminate(instanceId)` 在步骤边界生效：当前步骤被取消，实例进入 `TERMINATED`，后续步骤不再执行；
- 每次重试与失败原因（`ErrorKind` + message + attempt）都记录在 `StepRecord` 中并随状态持久化，结果可解释。

## 幂等规则

- **实例级**：`(workflowName, idempotencyKey)` 唯一。重复提交（包括并发同时提交）返回同一实例，不产生重复执行。
- **步骤级**：`StepContext.idempotencyKey()` 返回稳定的 `instanceId:stepName`，跨重试、跨重启不变；
  业务侧对外副作用（如扣款、发消息）应以此键做去重（如本地去重表/带键的缓存标记）。
- 引擎保证「已完成步骤不重跑」，但崩溃/超时导致「业务已生效但完成通知未持久化」时，该步骤会作为新 attempt 重跑，
  此时业务幂等是最终防线。

## REST 接口

```bash
# 启动（幂等）
curl -X POST http://localhost:8080/api/workflows/orderWorkflow/start \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-1001","input":"order-1001"}'

# 查询实例状态（含每个步骤的输入/输出/attempt/失败原因）
curl http://localhost:8080/api/workflows/instances/<instanceId>

# 终止
curl -X POST http://localhost:8080/api/workflows/instances/<instanceId>/terminate
```

数据目录由 `workflow.data-dir` 配置，默认 `./workflow-data`（每实例一个 JSON 文件，可直接打开审计）。

## 本地验证

```bash
# 全量单测（含实例/步骤/重试流水日志）
mvn test

# 单独验证某类场景
mvn test -Dtest=WorkflowEngineRetryTest       # 重试/超时/终止/失败原因区分
mvn test -Dtest=WorkflowEngineRecoveryTest    # 崩溃重启恢复、已完成步骤不重跑
mvn test -Dtest=WorkflowEngineConcurrencyTest # 幂等键并发去重、多实例并发隔离

# 启动后用上方 curl 手工验证
mvn spring-boot:run
```

测试覆盖：

1. **重启恢复**：step2 执行中杀掉引擎，新引擎同目录恢复后 step1 只执行一次、step2 重跑后完成；已完结实例不恢复；
2. **重复触发**：16 线程同幂等键并发提交收敛为 1 个实例、副作用恰好 1 次；完成后重复提交仍返回同一实例；
3. **并发执行**：20 个实例并发，步骤间数据不串扰，每实例每步恰好执行一次；
4. **失败语义**：可重试退避后成功、可重试耗尽 `FAILED`、不可重试立即失败、超时耗尽 `TIMED_OUT`、
   迟到完成通知不改变终态、终止后后续步骤不执行。
