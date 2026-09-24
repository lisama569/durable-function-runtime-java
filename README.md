# Durable Function Runtime (single-node, Java / Spring Boot)

一套单机运行的持久化函数工作流执行引擎：用本地文件状态承载执行过程，
使函数的调用、重试与并发触发在失败和重启后仍能得到一致且可解释的结果。
**仅依赖工程已有的 Spring Boot / Jackson 与本地文件系统，无任何外部服务或真实凭据。**

## 能力概览

- 有状态的顺序步骤编排；每实例的步骤输入 / 输出 / 尝试流水 / 事件流水持久化。
- 重复提交、重启恢复、回调乱序下，已完成步骤不重复执行，状态不回退。
- 区分可重试 / 不可重试 / 超时 / 中断错误（稳定 code）；退避与尝试次数有硬上限；
  超时或重试耗尽必然落入确定终态。
- 同一工作流多实例并发互不串扰；实例级锁 + 原子文件提交保证状态流转唯一；
  重复 / 迟到回调不导致计数错乱或重复副作用（`runOnce` 幂等效果）。
- 完整的实例 / 步骤 / 重试审计流水（日志 + JSON 事件）。

## 快速开始

```bash
mvn test                 # 16 个单测：恢复 / 重复触发 / 并发 / 超时 / 终止
mvn spring-boot:run      # 本地 HTTP 服务，见 docs/LOCAL_VERIFICATION.md 的 curl 示例
```

## 文档

- [执行模型：状态机、锁与原子提交](docs/EXECUTION_MODEL.md)
- [重试、超时、终止与幂等规则](docs/EXECUTION_MODEL.md#5-失败分类重试退避超时)
- [本地验证方法](docs/LOCAL_VERIFICATION.md)

## 关键代码

```
engine/
  DurableWorkflowEngine.java   # 驱动 / 回调 / 终止 / 恢复 / 幂等效果
  persistence/                 # StateStore 接口 + 本地文件原子实现
  function/                    # StepFunction / AsyncDispatcher / StepContext
  model/                       # 状态、错误类型、重试策略、流水
  web/WorkflowController.java  # 本地 REST 接口
demo/DemoWorkflows.java        # 开箱即用的演示工作流（含故障注入）
```
