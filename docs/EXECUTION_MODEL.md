# 单机持久化函数工作流执行引擎 — 执行模型

本工程在不依赖任何外部服务（无数据库 / 消息队列 / 第三方凭据）的前提下，
用**本地文件状态 + 每实例锁 + 原子提交**实现了一套可持久化、可重放、
可并发的有状态步骤编排引擎。

## 1. 核心抽象

| 类型 | 职责 |
| --- | --- |
| `WorkflowDefinition` | 一个工作流：有序的 `StepDefinition` 列表 + 可选输出投影 |
| `StepDefinition` | 单个步骤：名称、`RetryPolicy`、同步函数 `StepFunction` 或异步派发器 `AsyncDispatcher`、可选输入映射 |
| `WorkflowInstance` | 一次运行的根聚合：状态、各步骤 `StepState`、尝试流水 `AttemptRecord`、事件流水 `WorkflowEvent`、去重键、版本号 |
| `DurableWorkflowEngine` | 启动 / 驱动 / 回调 / 终止 / 恢复，并实现幂等副作用 `IdempotenceService` |
| `StateStore` / `FileStateStore` | 持久化抽象与本地文件实现 |
| `WorkflowRegistry` | 工作流定义注册表（定义属于代码，重启后需重新注册） |

## 2. 持久化布局（`durable.engine.storage-dir`，默认 `target/durable-state`）

```
instances/<instanceId>.json        # 权威聚合，单文件原子替换
effects/<instanceId>/<step>/<sha256(effectKey)>.json   # 幂等副作用结果
```

- 每个实例一个 JSON 文件；提交采用 **临时文件 + `ATOMIC_MOVE` 替换**，
  崩溃只会看到旧版本或新版本，绝不会读到写了一半的聚合。
- 目录列举过滤 `.` 开头的临时文件，避免并发提交期间误读。
- 每个实例一把 `ReentrantLock`：**同一实例的一切状态流转串行**，
  不同实例完全不互斥。
- 聚合带单调 `version`，每次提交 +1，用于审计与乐观防护。

## 3. 状态机

### 实例 `InstanceStatus`
`RUNNING → COMPLETED | FAILED | TERMINATED`（后三者为终态，**只能进入、不能回退**）

### 步骤 `StepStatus`
```
PENDING ──开始尝试──► RUNNING(同步) / WAITING(异步)
                          │
            成功 ───────► COMPLETED ──(驱动器)──► 下一步 / WORKFLOW_COMPLETED
            可重试失败 ──► RETRY_WAIT(notBefore=now+backoff) ──到期──► 再次尝试
            超时 ───────► RETRY_WAIT（TIMEOUT，仍占重试预算）
            不可重试 / 预算耗尽 ─► FAILED（实例随之进入终态）
重启时遗留 RUNNING ──恢复──► INTERRUPTED → RETRY_WAIT
```

## 4. 顺序推进与“恰好一次”语义

- 驱动器 `drive()` 每次只推进当前步骤；完成的步骤永久跳过。
- **同步步骤在单次持锁提交内完成**“登记尝试边界 → 执行业务 → 落定结果/推进索引”。
  因此任何并发回调 / 恢复 / 终止都不可能在执行过程中把在途尝试误判为中断，
  也不可能把已完成步骤回滚。
- **异步步骤**先持久化 `WAITING + 完成令牌 token`（token 与 `实例/步骤/尝试号`
  绑定并存入尝试流水），再在锁外派发；之后只接受携带当前活跃 token 的回调。
- 回调成功时在**同一把锁、同一提交**内把步骤置完成并推进工作流，
  消除“已完成但未推进”的观察窗口。

## 5. 失败分类、重试、退避、超时

| `ErrorType` | 含义 | 是否重试 |
| --- | --- | --- |
| `RETRYABLE` | 瞬时错误（业务抛 `RetryableFailureException`） | 是 |
| `NON_RETRYABLE` | 永久错误（业务抛 `NonRetryableFailureException`），立即失败 | 否 |
| `TIMEOUT` | 尝试超过 `timeoutMs` | 是（计入预算） |
| `INTERRUPTED` | 重启时遗留的在途尝试 | 是（计入预算） |
| `RETRIES_EXHAUSTED` | 重试预算耗尽的终态包装（保留最后一次稳定 code） | 终态 |
| `TERMINATED` | 显式终止 | 终态 |

未识别异常一律按 `RETRYABLE / UNEXPECTED_ERROR` 处理，避免非预期错误卡死。

`RetryPolicy`（均有**硬上限**，防止配置失控）：

| 参数 | 含义 | 硬上限 |
| --- | --- | --- |
| `maxAttempts` | 最大尝试次数（含首次） | 20 |
| `initialBackoffMs` | 首次重试退避基数 | — |
| `backoffMultiplier` | 指数倍率（≥1） | — |
| `maxBackoffMs` | 退避封顶 | 60 000 ms |
| `timeoutMs` | 单次尝试超时 | 300 000 ms |

第 N 次尝试前的退避 = `min(initialBackoff × multiplier^(N-2), maxBackoff)`。

**超时由后台 ticker（或测试中的确定性 `recover`）驱动**：发现
`now ≥ attemptDeadline` 即把尝试标记 `TIMED_OUT`，进入退避或终态。
无论成功、失败、超时、终止，最终都收敛到明确状态，**不存在中间态悬挂**。

## 6. 幂等规则

1. **重复启动**：相同 `instanceId` 重复提交只记录 `START_DEDUPED` 审计事件，
   返回既有状态，绝不重跑任何步骤；`dedupeKey` 亦被持久化去重。
2. **重复 / 乱序 / 迟到回调**：
   - token 解析与状态判定在**同一实例锁内**原子完成；
   - 非当前活跃 token（已完成尝试 / 未知 / 终态后到达）只追加
     `CALLBACK_IGNORED`，不改业务状态、不影响计数；
   - `dedupeKey` 相同的回调记录 `CALLBACK_DEDUPED`。
3. **业务副作用幂等**：步骤通过 `StepContext.runOnce(effectKey, effect)`
   执行外部效果；结果按 `(实例, 步骤, effectKey)` 落盘，重试 / 重放直接
   返回已存结果，**同一副作用最多执行一次**。
4. **重启恢复**：
   - 已 `COMPLETED`：直接跳过，不重放；
   - `RETRY_WAIT`：保留 `notBefore`，继续遵守退避；
   - `WAITING`（异步）：继续等待回调或超时；
   - 遗留 `RUNNING`：记为 `INTERRUPTED` 并按原预算重试。

## 7. 并发模型

- 状态聚合的所有变更都走 `StateStore.mutate`（实例级互斥 + 原子文件提交）。
- 同步步骤业务逻辑在锁内执行（步骤函数被设计为纯本地、确定性逻辑；
  外部交互必须走 `runOnce` 或异步派发），因此无需两阶段锁即可保证：
  - 多实例并发互不串扰（输入 / 输出按实例隔离）；
  - 并发回调下完成计数恒为一次；
  - 迟到通知不会重复副作用。
- 后台：一个守护线程 ticker 周期驱动全部实例（退避到期 / 超时），
  一个缓存线程池承接异步驱动；`autoStart=false` 时引擎退化为纯同步、
  确定性执行，便于单测。

## 8. 可解释性（审计流水）

每次状态变化都追加一条不可变 `WorkflowEvent`（序号、时间、步骤、尝试号、
类型、详情），每个尝试有一条 `AttemptRecord`（开始/结束时间、结果、
错误类型、稳定 code、消息）。日志 logger 名 `DURABLE-WORKFLOW`，
单测以 `DURABLE-TEST` 打印实例 / 步骤 / 重试流水，例如：

```
[wf-retry-1] step=flaky attempt=1 event=STEP_ATTEMPT_START detail=sync attempt 1/3
[wf-retry-1] step=flaky attempt=2 event=STEP_RETRY_SCHEDULED detail=backoffMs=50 reason=UPSTREAM_BUSY
[wf-retry-1] step=flaky attempt=3 event=STEP_COMPLETED detail=sync success
```
