# 持久化函数工作流执行引擎（单机版）

一套单机运行的持久化工作流引擎：业务逻辑编排为节点图——既支持按顺序推进的**串行步骤**，
也支持依据上一步输出**选择分支**，以及一个步骤**并行扇出多路工作、汇合一次后继续**。
每个实例的活动输入、输出、尝试次数、分支选择与并行结果都持久化在**本地文件**中。
不依赖任何外部服务或真实凭据，仅使用 Spring Boot Web 与 Jackson。

## 模块结构

| 位置 | 职责 |
| --- | --- |
| `domain/` | 状态模型：`WorkflowInstance`、`StepRecord`、`WorkflowStatus`、`StepStatus`、`ErrorKind`、`RetryPolicy` |
| `workflow/` | 定义模型：`WorkflowDef`、`WorkflowDefinition`（旧串行）、`ControlFlowDefinition`（新）、`StepHandler`、`StepContext` |
| `workflow/node/` | 控制流节点：`ActivityNode`、`ChoiceNode`/`ChoiceBranch`、`ParallelNode`/`ParallelBranch`、`JoinHandler`、`BranchResults` |
| `engine/CompiledFlow` | 定义编译：稳定节点 id、结构解析、同序列后继关系 |
| `engine/FileWorkflowStore` | 本地 JSON 文件持久化：临时文件 + 原子移动，乐观版本号并发控制 |
| `engine/WorkflowEngine` | 事件驱动执行引擎：单线程事件循环 + worker 线程池 + 定时调度 + 并行名额 |
| `web/WorkflowController` | REST 接口：幂等启动 / 查询 / 终止 |
| `web/EngineConfig` | Spring 装配与两个示例工作流（串行 `orderWorkflow`、分支+并行 `fulfillmentWorkflow`） |

## 定义模型

一个工作流是一个顶层**节点序列**，节点有三种：

- **`ActivityNode(name, handler, retryPolicy)`**：执行一段业务逻辑，语义与串行步骤完全一致；
- **`ChoiceNode(name, branches)`**：依据上游活动输出，在多条 `ChoiceBranch(label, when, body)` 中
  选择**唯一**一条执行。`when` 为 `null` 的分支是兜底分支（至多一条）；分支体本身还是一个节点序列，
  因此可以嵌套；
- **`ParallelNode(name, branches, joinName, joinHandler, joinRetryPolicy)`**：同时拉起多路
  `ParallelBranch(label, activityName, handler, retryPolicy)`，全部成功后由 `joinHandler` **汇合一次**。

用 builder 组装：

```java
engine.register(ControlFlowDefinition.name("demo")
        .then(new ActivityNode("classify", (ctx, in) -> in, RetryPolicy.defaults()))
        .then(new ChoiceNode("route", List.of(
                new ChoiceBranch("vip", "vip"::equals,
                        List.of(new ActivityNode("vipStep", handler, policy))),
                new ChoiceBranch("other", null,                  // 兜底分支
                        List.of(new ActivityNode("otherStep", handler, policy))))))
        .then(new ParallelNode("settle", List.of(
                new ParallelBranch("a", "doA", handlerA, policy),
                new ParallelBranch("b", "doB", handlerB, policy)),
                "merge",
                (ctx, results) -> results.get("a") + "+" + results.get("b"),
                RetryPolicy.defaults()))
        .then(new ActivityNode("archive", handler, policy))
        .build());
```

汇合时通过 `BranchResults` 取结果：`labels()`/`outputs()` 按**分支定义顺序**排列，
`get(label)` 按标签取；因此同样的分支输出，无论各路完成先后，汇合结果都相同。

## 执行模型

1. **启动**：`start(...)` 创建实例，激活根序列首节点（活动直接就绪 / 分支立即选路 / 并行立即扇出），
   状态先一次性落盘再投递派发。
2. **状态流转唯一**：所有状态迁移都发生在**单线程事件循环**中；业务活动在 worker 线程池执行，
   完成后投递携带 `(instanceId, nodeId, attempt)` 的完成通知。
3. **分支选择**：活动完成后若后继是 ChoiceNode，用其输出在事件循环中谓词匹配，命中分支的首节点就绪，
   **未选分支子树内的活动一律标记 `SKIPPED`**——不执行、不阻塞，也不影响实例终态与结果。
4. **并行扇出/汇合**：后继是 ParallelNode 时，一次性把全部分支活动置为就绪并持久化；
   分支在 worker 上并发执行，输出落入 `branchOutputs`；最后一路完成时按**定义顺序**重建汇合输入，
   汇合活动（join）**恰好入队一次**。join 的输入/输出与完成顺序无关。
5. **持久化优先 & 原子推进**：「活动完成 + 后继就绪 + 跳过标记 + 汇合输入 + 终态」在一次状态更新中
   原子落盘，之后才投递内存派发，杜绝「写一半崩溃导致卡死或重复汇合」。
6. **完成通知去重**：处理前校验「实例 RUNNING + 活动 RUNNING + attempt 匹配」，
   迟到/重复通知（旧 attempt 的迟到结果、超时后才返回、终止后到达）一律丢弃，终态不可逆。
7. **重启恢复**：启动时扫描所有 `RUNNING` 实例——已 COMPLETED 的活动（含已完成的并行分支）
   **不会重跑**；崩溃时处于 RUNNING 的活动作为新 attempt 重跑；持久化就绪队列中未跑的活动继续派发；
   并对「选择/扇出已落盘但派发事件丢失」的边界做控制流自愈。业务副作用仍需以 `StepContext.idempotencyKey()` 幂等。
8. **旧版状态迁移**：升级前落盘的纯串行实例（只有 `steps` + `currentStepIndex`）在首次 `recover()` 时
   自动补齐节点 id 与新控制流字段，按旧的当前步骤继续，无需人工迁移。

终态：`COMPLETED` / `FAILED` / `TIMED_OUT` / `TERMINATED`，均不可逆。
活动状态：`PENDING` / `RUNNING` / `COMPLETED` / `FAILED` / `SKIPPED`。

## 重试、超时、终止与并行失败规则

- 活动抛 `RetryableException` → `RETRYABLE`，按指数退避重试；抛 `NonRetryableException` 或未分类异常
  → 立即 `FAILED`，不重试；执行超 `stepTimeoutMillis` → `TIMEOUT`，取消 worker，可重试则退避，否则 `TIMED_OUT`；
- 并行的**每一路分支沿用它自己的 `RetryPolicy`**（尝试次数、退避、超时彼此独立）；join 也有独立策略；
- **任一路分支彻底失败或超时耗尽 → 整个实例立即进入确定终态（FAILED / TIMED_OUT）**，
  事件循环取消同一实例**所有其他在跑活动**（中断 worker、撤销其就绪项），汇合不得执行、后续节点不再运行，
  不留后台任务继续改状态；迟到结果不会使终态回退；
- `terminate(instanceId)` 立即生效：取消该实例全部在跑活动，进入 `TERMINATED`；
- 每次重试与失败原因（`ErrorKind` + message + attempt）都记录在对应 `StepRecord` 中并随状态持久化。

## 并行规模上限（防止饿死）

- `workflow.max-branches-per-instance`（默认 16）：单个并行节点允许的分支数上限，**注册期**即校验拒绝；
- `workflow.max-parallel-in-flight`（默认 8）：整个引擎同时在跑的并行分支活动数上限。
  超出的就绪分支在内存 **FIFO 队列**中等待，有名额释放（worker 结束）后按序派发；
  顺序活动与 join 不占名额。多个实例一起跑时，全局名额按 FIFO 在实例间公平流转，
  实例状态各自持久化、互不串扰，也不会被某一个大扇出实例永久饿死。

## 幂等规则

- **实例级**：`(workflowName, idempotencyKey)` 唯一，并发重复提交收敛为同一实例。
- **活动级**：`StepContext.idempotencyKey()` 返回稳定的 `instanceId:nodeId`，
  节点 id 由定义位置确定（顶层为活动名；分支体内为 `<choiceId>/b/<label>/...`；
  并行分支为 `<parallelId>/p/<label>`；汇合为 `<parallelId>/join`），跨重试、跨重启不变。
- 引擎保证「已完成活动不重跑、汇合只跑一次」，但崩溃/超时导致「业务已生效而完成通知未持久化」时，
  该活动会作为新 attempt 重跑，业务幂等仍是最终防线。

## 与原串行流程的兼容性

- 旧的 `new WorkflowDefinition(name, List<StepDefinition>)` 与 `engine.register(...)`、
  REST 接口、`WorkflowInstance`/`StepRecord` 字段全部保留，旧工作流**无需改动**，行为与升级前一致；
- 旧定义在内部被编译成「全是顶层 ActivityNode」的图，每个 step 的节点 id 就是其 step 名，
  因此旧的 `instanceId:stepName` 幂等键保持稳定；
- 升级前已落盘的运行中实例可被新引擎自动恢复（见执行模型第 8 点）。

## REST 接口

```bash
# 启动（幂等）
curl -X POST http://localhost:8080/api/workflows/fulfillmentWorkflow/start \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-1001","input":"vip"}'

# 查询实例状态（含每个活动的 nodeId/输入/输出/attempt/失败原因，choices 与 branchOutputs）
curl http://localhost:8080/api/workflows/instances/<instanceId>

# 终止
curl -X POST http://localhost:8080/api/workflows/instances/<instanceId>/terminate
```

数据目录由 `workflow.data-dir` 配置，默认 `./workflow-data`（每实例一个 JSON 文件，可直接打开审计）。

## 本地验证

```bash
# 全量单测
mvn test

# 分支选择：命中/兜底、未选分支 SKIPPED、不阻塞、计数确定
mvn test -Dtest=WorkflowBranchTest
# 并行汇合：乱序完成结果一致、join 恰好一次、每路恰好一次、同扇出输入
mvn test -Dtest=WorkflowParallelTest
# 并行失败/超时/终止：确定终态、取消其他在跑分支、迟到结果不改终态
mvn test -Dtest=WorkflowParallelFailureTest
# 并行中途重启：已完成分支不重跑、只补未完成分支、join 只跑一次
mvn test -Dtest=WorkflowParallelRecoveryTest
# 扇出上限/全局名额/多实例隔离不饿死
mvn test -Dtest=WorkflowParallelLimitTest
# 原有串行能力
mvn test -Dtest=WorkflowEngineRetryTest
mvn test -Dtest=WorkflowEngineRecoveryTest
mvn test -Dtest=WorkflowEngineConcurrencyTest

# 启动后用上方 curl 手工验证（input 传 vip / standard / 其他值可观察不同分支）
mvn spring-boot:run
```

测试覆盖（共 25 个用例）：

1. **分支选择**：命中分支执行、其余分支 SKIPPED 且计数为 0；无条件命中走兜底；多次运行计数确定、收尾照常；
2. **并行汇合**：完成顺序被打乱（y,z,x 等 4 种延迟方案）汇合输出恒为定义序 `X|Y|Z`；join 恰好 1 次；
   每路分支恰好 1 次；各路收到相同扇出输入；完成后无 PENDING/RUNNING 残留、并行组关闭；
3. **并行失败/超时/终止**：一路不可重试失败 → FAILED 且其他在跑分支被中断、join 不执行、后续不运行；
   一路重试耗尽 → FAILED（RETRYABLE 归因，次数受 maxAttempts 限制）；一路超时耗尽 → TIMED_OUT；
   终止取消全部在跑分支；迟到结果均不改终态；
4. **并行中途重启**：x/y 已完成、z 在途时杀掉引擎，重启后 x/y 各仍 1 次、z 补跑 1 次、
   join 总计 1 次、最终输出 `done:X|Y|Z`；
5. **规模与隔离**：注册期拒绝超限扇出；全局在途名额上限被严格遵守（4 路、名额 2，峰值=2，其余排队）；
   12 实例 × 3 路并发全部完成、每路恰好 1 次、汇合结果只含本实例数据、无实例被饿死；
6. **原有串行用例全部保留通过**：重启恢复、16 线程幂等去重、20 实例隔离、重试/超时/终止/失败归因。
