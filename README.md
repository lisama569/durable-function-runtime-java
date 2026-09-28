# 持久化函数工作流执行引擎（单机版）

一套单机运行的持久化工作流引擎：业务逻辑编排为步骤（Task）、分支选择（Choice）与并行扇出/汇合（Parallel）。
每个实例的步骤输入、输出、尝试次数、执行位置与状态持久化在**本地文件**中。
不依赖任何外部服务或真实凭据，仅使用 Spring Boot Web 与 Jackson。

## 模块结构

| 位置 | 职责 |
| --- | --- |
| `domain/` | 状态模型：`WorkflowInstance`、`StepRecord`、`ExecutionState`、`ParallelCursor`、各状态/错误枚举、`RetryPolicy` |
| `workflow/` | 定义模型：`WorkflowDefinition`（旧，纯串行）、`FlowDefinition`（新，节点式）、`TaskNode`、`ChoiceNode`、`ParallelNode`、`FlowBranch`、`JoinHandler`、`BranchSelector` |
| `engine/FileWorkflowStore` | 本地 JSON 文件持久化：临时文件 + 原子移动，乐观版本号并发控制 |
| `engine/WorkflowEngine` | 事件驱动执行引擎：单线程事件循环 + 有界 worker 线程池 + 定时调度 |
| `web/WorkflowController` | REST 接口：幂等启动 / 查询 / 终止 |
| `web/EngineConfig` | Spring 装配与两个示例：串行 `orderWorkflow`、分支+并行 `orderFlowV2` |

## 两种定义方式

### 1. 纯串行（旧接口，完全兼容）

```java
engine.register(new WorkflowDefinition("orderWorkflow", List.of(
        new StepDefinition("validate", handler, RetryPolicy.defaults()),
        new StepDefinition("charge",   handler, RetryPolicy.defaults()),
        new StepDefinition("ship",     handler, RetryPolicy.defaults()))));
```

### 2. 节点式（新接口，支持分支与并行）

```java
engine.registerFlow(new FlowDefinition("orderFlowV2", List.of(
        new TaskNode("classify", handler, RetryPolicy.defaults()),

        // 分支选择：selector 依据上游输出返回分支 label；未选中分支整体 SKIPPED
        new ChoiceNode("shipping",
                out -> out.startsWith("express") ? "express" : "standard",
                List.of(new FlowBranch("express",  List.of(new TaskNode("expressPick",  h, policy))),
                        new FlowBranch("standard", List.of(new TaskNode("standardPick", h, policy))))),

        // 并行扇出：同时拉起 reserve / sendMessage 两路，全部完成后 join 恰好一次
        new ParallelNode("notify",
                List.of(new FlowBranch("reserve", List.of(new TaskNode("reserve",     h, policy))),
                        new FlowBranch("message", List.of(new TaskNode("sendMessage", h, policy)))),
                (joinCtx, outputsInBranchOrder) -> String.join("+", outputsInBranchOrder)),

        new TaskNode("archive", handler, RetryPolicy.defaults()))));
```

> 当前版本分支内部只包含 `TaskNode`（顺序任务列表），不支持在分支里再嵌套 Choice/Parallel。
> 顶层节点仍按顺序推进，因此「并行」与「分支」可以与普通步骤前后串联。

## 执行模型

1. **启动**：`start(workflowName, idempotencyKey, input)` 创建实例，全部步骤记录（含每个分支任务与一个 `*.join` 汇合记录）
   先落盘，再投递恢复/推进事件。执行位置记录在 `ExecutionState`（顶层游标 + 选择分支游标 + 各并行分支游标）。
2. **状态流转唯一**：所有状态迁移（含选择哪条分支、标记 SKIPPED、推进游标、执行汇合）都发生在**单线程事件循环**中；
   业务任务在有界 worker 线程池执行，完成后投递携带 `(instanceId, path, attempt)` 的通知。
3. **顺序推进**：任务 COMPLETED 并持久化后才推进；上游任务输出作为下游节点输入。
4. **分支选择（Choice）**：上游任务完成后，在事件循环中同步运行 `BranchSelector.select(upstreamOutput)`，
   返回唯一分支 label：
   - 被选中分支的任务正常执行；**其余分支的全部任务原子标记为 `SKIPPED`，不执行、不卡住流程、不参与结果**；
   - selector 抛异常或返回未知 label → 实例立即 `FAILED`（不重试，视为定义/数据错误）。
5. **并行扇出/汇合（Parallel）**：
   - 节点激活时**同时**拉起每条分支的首个任务；各分支内部顺序推进，互不等待；
   - 每条分支完成后独立持久化；**全部分支完成后汇合恰好执行一次**；
   - `JoinHandler` 收到的输出列表**按分支定义顺序排列**，因此汇合结果与各路完成的先后顺序无关；
   - 汇合逻辑在事件循环中同步执行，其成功与新的顶层游标在同一次持久化中落盘（崩溃时按既有 at-least-once 重放，业务汇合副作用需幂等，`JoinContext.idempotencyKey()` 稳定）。
6. **持久化优先**：每次状态迁移先写盘后推进。文件写入采用「临时文件 + 原子 rename」，更新带 `version` 乐观锁。
7. **完成通知去重**：事件处理前校验「实例 RUNNING + 记录 RUNNING + attempt 匹配 + 该路径正位于当前执行头」，
   任何重复/迟到通知（重试旧 attempt、超时后才返回、终态后到达）都被丢弃，终态不可逆。
8. **重启恢复**：启动时扫描所有 `RUNNING` 实例：
   - 已 COMPLETED 的任务/分支**不会重复执行**；崩溃时处于 RUNNING 的任务退回 PENDING，以新 attempt 补跑（副作用需幂等）；
   - 并行节点中已完成的分支直接跳过，只补跑未完成的分支，随后仍只汇合一次；
   - 旧的纯串行实例（无 `ExecutionState`）在恢复时按当前 `currentStepIndex` 惰性迁移，历史数据无需手工处理。

步骤状态：`PENDING` / `RUNNING` / `COMPLETED` / `FAILED` / `SKIPPED`（未选分支）/ `CANCELLED`（被终态失败/终止停下的任务）。
实例终态：`COMPLETED` / `FAILED` / `TIMED_OUT` / `TERMINATED`，均不可逆。

## 重试、超时、失败与终止（并行下同样适用）

- 任务抛 `RetryableException` → 退避重试（`initialBackoff * 2^(attempt-1)`，封顶 `maxBackoff`）；
- 抛 `NonRetryableException` 或未分类异常 → **立即** `FAILED`；
- 执行时长超过 `stepTimeoutMillis` → 取消 worker，可重试则退避，否则实例 `TIMED_OUT`；
- 尝试次数硬性上限 `maxAttempts`；重试耗尽落入确定终态，不停留中间态。

并行扇出时的额外保证：

- **任一路**彻底失败或超时 → 整个实例立即进入确定终态（`FAILED`/`TIMED_OUT`）：
  事件循环取消该实例**所有其他仍在运行**的任务（`Future.cancel(true)`），未执行的任务标记 `CANCELLED`，
  汇合记录也置 `CANCELLED`，不留下后台任务继续改状态；其他分支迟到的完成通知一律丢弃。
  已正常 COMPLETED 的兄弟分支保留其结果（真实有效），不会被改写。
- **扇出上限**：单个 `ParallelNode` 的分支数若超过 `workflow.max-parallel-branches`（默认 8），该实例立即 `FAILED`，
  不拉起任何任务，避免一个实例铺开过多。
- **全局天花板**：worker 线程数由 `workflow.worker-threads`（默认 16）统一限制，任务在 FIFO 队列排队而不是无界扩张，
  避免并行实例把同进程其他实例饿死。实例间状态按文件与内存索引严格隔离，不串扰、不互相拖垮。
- `terminate(instanceId)` 在并行期间同样生效：取消所有在跑任务，后续步骤/汇合不再执行，实例进入 `TERMINATED`。

## 幂等规则

- **实例级**：`(workflowName, idempotencyKey)` 唯一，重复（含并发）提交返回同一实例。
- **任务级**：`StepContext.idempotencyKey()` 返回稳定的 `instanceId:path`（如 `fan[0].reserve`），跨重试、跨重启、跨并行分支唯一；
  汇合可用 `JoinContext.idempotencyKey()`（`instanceId:nodeName:join`）。业务侧对外副作用应以此键去重。
- 引擎保证「已完成任务/分支不重跑」，但崩溃/超时导致「业务已生效而完成通知未持久化」时该任务会以新 attempt 重跑，业务幂等是最终防线。

## 配置

`src/main/resources/application.properties`：

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `workflow.data-dir` | `./workflow-data` | 实例 JSON 文件目录 |
| `workflow.worker-threads` | `16` | 全局 worker 线程数（并行总闸） |
| `workflow.max-parallel-branches` | `8` | 单实例单次并行扇出分支上限 |

## REST 接口

```bash
# 启动（幂等）
curl -X POST http://localhost:8080/api/workflows/orderFlowV2/start \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-1001","input":"premium-order-1001"}'

# 查询实例（含每个任务/分支/汇合的 path、状态、输入/输出、attempt、失败原因）
curl http://localhost:8080/api/workflows/instances/<instanceId>

# 终止
curl -X POST http://localhost:8080/api/workflows/instances/<instanceId>/terminate
```

## 本地验证

```bash
# 全量单测（25 个）
mvn test

# 按场景单独验证
mvn test -Dtest=WorkflowEngineBranchTest     # 分支选择、未选分支 SKIPPED、selector 失败、分支内重试
mvn test -Dtest=WorkflowEngineParallelTest   # 并行乱序汇合、崩溃只补未完成分支、失败/超时取消、上限、终止、多实例隔离
mvn test -Dtest=WorkflowEngineRetryTest      # 重试/超时/终止/失败原因（串行）
mvn test -Dtest=WorkflowEngineRecoveryTest   # 崩溃重启恢复、已完成步骤不重跑
mvn test -Dtest=WorkflowEngineConcurrencyTest# 幂等键并发去重、多实例并发隔离

# 启动后用上方 curl 手工验证（orderWorkflow 为旧串行示例，orderFlowV2 为分支+并行示例）
mvn spring-boot:run
```

测试覆盖：

1. **分支选择**：两条分支分别按上游输出选中；未选分支任务落 `SKIPPED` 且不执行，最终结果只来自选中分支；
   selector 抛异常 / 返回未知 label → 立即 FAILED；选中分支任务沿用退避重试且上游不重跑。
2. **并行汇合**：两种完成顺序（b0 慢 / b1 慢）下汇合输出都为 `B0|B1` 且 join 恰好一次。
3. **崩溃恢复**：一路已完成、另一路执行中杀掉进程；重启后已完成路不重跑，在跑路以新 attempt 补跑，汇合只执行一次。
4. **一路失败**：`NonRetryableException` 使实例 FAILED；其他在跑路收到取消信号，任务/汇合置 CANCELLED；
   放行后迟到完成通知不改变终态、不产生输出、不再触发汇合。
5. **一路超时**：超时耗尽实例 TIMED_OUT，已完成兄弟保留 COMPLETED，汇合 CANCELLED。
6. **扇出上限**：分支数超过单实例上限时立即 FAILED，且一个任务都不执行。
7. **分支内重试**：并行某路可重试失败重试到成功，汇合仍只执行一次，attempt 计数正确。
8. **并行期间终止**：两路在跑时终止 → TERMINATED，所有路与后续步骤取消，汇合不执行。
9. **多实例隔离**：8 个实例并行各自两路，结果不串扰，每路恰好一次、每实例汇合一次。
10. **多任务分支**：分支内含多个任务时严格顺序推进（后者拿到前者输出），与其他分支并行、慢路不影响汇合顺序。
11. **汇合点崩溃**：两路业务已执行但完成通知未处理时崩溃，恢复后按 at-least-once 补跑而汇合仍只执行一次。
10. **原有用例**：重启恢复、幂等并发去重、20 实例并发隔离、重试/超时/终止语义全部保持通过；旧串行工作流行为不变。
