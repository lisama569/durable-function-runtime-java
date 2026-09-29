package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.NonRetryableException;
import com.github.lisama569.durablefunctionruntimejava.engine.RetryableException;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.ParallelNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.TaskNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并行扇出/汇合：乱序完成结果一致、汇合一次、崩溃只补跑未完成分支、失败/超时取消兄弟。 */
class WorkflowEngineParallelTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngineParallelTest.class);
    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;

    private static StepRecord findRecord(WorkflowInstance instance, String path) {
        return instance.getSteps().stream().filter(s -> path.equals(s.getPath())).findFirst().orElseThrow();
    }

    private boolean waitRecord(WorkflowEngine engine, String id, String path, StepStatus status, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            StepRecord record = findRecord(engine.get(id).orElseThrow(), path);
            if (record.getStatus() == status) return true;
            Thread.sleep(10);
        }
        return false;
    }

    /** 两个单任务分支 + 汇合；分支可被 gate 阻塞、可加延迟，用于乱序/失败等场景。 */
    private static FlowDefinition twoBranchWorkflow(
            AtomicInteger b0Calls, AtomicInteger b1Calls, AtomicInteger joinCalls,
            Map<String, Long> delayByBranch,
            Map<String, CountDownLatch> enterGate,
            Map<String, CountDownLatch> releaseGate) {

        TaskNode b0 = new TaskNode("b0", (ctx, input) -> {
            b0Calls.incrementAndGet();
            CountDownLatch enter = enterGate == null ? null : enterGate.get("b0");
            if (enter != null) enter.countDown();
            CountDownLatch release = releaseGate == null ? null : releaseGate.get("b0");
            if (release != null) release.await(10, TimeUnit.SECONDS);
            Long delay = delayByBranch == null ? null : delayByBranch.get("b0");
            if (delay != null && delay > 0) Thread.sleep(delay);
            return "B0";
        }, FAST);

        TaskNode b1 = new TaskNode("b1", (ctx, input) -> {
            b1Calls.incrementAndGet();
            CountDownLatch enter = enterGate == null ? null : enterGate.get("b1");
            if (enter != null) enter.countDown();
            CountDownLatch release = releaseGate == null ? null : releaseGate.get("b1");
            if (release != null) release.await(10, TimeUnit.SECONDS);
            Long delay = delayByBranch == null ? null : delayByBranch.get("b1");
            if (delay != null && delay > 0) Thread.sleep(delay);
            return "B1";
        }, FAST);

        return new FlowDefinition("fan", List.of(
                new ParallelNode("fan", List.of(
                        new FlowBranch("zero", List.of(b0)),
                        new FlowBranch("one", List.of(b1))),
                        orderedJoin(joinCalls))));
    }

    /** 汇合：按定义顺序拼接，调用次数用计数器断言恰好一次。 */
    private static com.github.lisama569.durablefunctionruntimejava.workflow.JoinHandler orderedJoin(AtomicInteger joinCalls) {
        return (ctx, outputs) -> {
            joinCalls.incrementAndGet();
            log.info("[instance={}] join invoked with {}", ctx.instanceId(), outputs);
            return String.join("|", outputs);
        };
    }

    // ---------------------------------------------------------------- 乱序完成

    @Test
    void joinResultIsIndependentOfBranchCompletionOrder() {
        runJoinWithDelays(400L, 30L);
        runJoinWithDelays(30L, 400L);
    }

    private void runJoinWithDelays(long delayB0, long delayB1) {
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            AtomicInteger b0Calls = new AtomicInteger();
            AtomicInteger b1Calls = new AtomicInteger();
            AtomicInteger joinCalls = new AtomicInteger();
            engine.registerFlow(twoBranchWorkflow(b0Calls, b1Calls, joinCalls,
                    Map.of("b0", delayB0, "b1", delayB1), null, null));
            WorkflowInstance instance = engine.start("fan", "order-" + delayB0 + "-" + delayB1, "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals("B0|B1", findRecord(terminal, "fan.join").getOutput(),
                    "汇合按分支定义顺序聚合，与完成先后无关");
            assertEquals(1, joinCalls.get(), "汇合只能执行一次");
            assertEquals(1, b0Calls.get());
            assertEquals(1, b1Calls.get());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 崩溃恢复

    @Test
    void crashedParallelRunResumesOnlyUnfinishedBranchesAndJoinsOnce() throws Exception {
        AtomicInteger b0Calls = new AtomicInteger();
        AtomicInteger b1Calls = new AtomicInteger();
        AtomicInteger joinCalls = new AtomicInteger();
        CountDownLatch b1Entered = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);

        WorkflowEngine engine1 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine1.registerFlow(recoveryWorkflow(b0Calls, b1Calls, joinCalls, b1Entered, releaseB1));
        WorkflowInstance started = engine1.start("fanRecover", "key-crash", "in");

        assertTrue(b1Entered.await(5, TimeUnit.SECONDS));
        assertTrue(waitRecord(engine1, started.getId(), "fan[0].b0", StepStatus.COMPLETED, 5_000),
                "分支 0 应已完成并持久化");
        engine1.shutdown(); // 杀掉：分支 1 正在执行
        assertEquals(1, b0Calls.get());

        WorkflowEngine engine2 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            engine2.registerFlow(recoveryWorkflow(b0Calls, b1Calls, joinCalls, b1Entered, releaseB1));
            assertEquals(1, engine2.recover());
            releaseB1.countDown(); // 放行恢复后补跑的分支 1

            WorkflowInstance terminal = engine2.awaitTerminal(started.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(1, b0Calls.get(), "已完成分支重启后不得重复执行");
            assertEquals(2, b1Calls.get(), "崩溃时在跑的分支应作为新 attempt 补跑一次");
            assertEquals(1, joinCalls.get(), "汇合在恢复后只执行一次");
            assertEquals("B0|B1", findRecord(terminal, "fan.join").getOutput());
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan[0].b0").getStatus());
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan[1].b1").getStatus());
        } finally {
            engine2.shutdown();
        }
    }

    private static FlowDefinition recoveryWorkflow(AtomicInteger b0Calls, AtomicInteger b1Calls,
                                                  AtomicInteger joinCalls,
                                                  CountDownLatch b1Entered, CountDownLatch releaseB1) {
        return new FlowDefinition("fanRecover", List.of(
                new ParallelNode("fan", List.of(
                        new FlowBranch("zero", List.of(new TaskNode("b0", (ctx, input) -> {
                            b0Calls.incrementAndGet();
                            return "B0";
                        }, FAST))),
                        new FlowBranch("one", List.of(new TaskNode("b1", (ctx, input) -> {
                            b1Calls.incrementAndGet();
                            b1Entered.countDown();
                            releaseB1.await(10, TimeUnit.SECONDS);
                            return "B1";
                        }, FAST)))),
                        orderedJoin(joinCalls))));
    }

    // ---------------------------------------------------------------- 一路失败取消全部

    @Test
    void oneBranchFailingFailsInstanceAndCancelsOtherBranchesWithoutLateWrites() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        AtomicBoolean b0Interrupted = new AtomicBoolean();
        AtomicBoolean b2Interrupted = new AtomicBoolean();
        CountDownLatch slowEntered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        // 两路慢任务都已进入处理体后再放行失败路：确保被取消的是「真正在跑」的兄弟任务，
        // 而不是还没被调度到的任务（调度竞态会让该用例偶发失败）。
        CountDownLatch failProceed = new CountDownLatch(1);

        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("fanFail", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("zero", List.of(new TaskNode("b0", (ctx, input) -> {
                                slowEntered.countDown();
                                awaitIgnoringInterrupts(release, b0Interrupted);
                                return "late-b0"; // 即使最终返回，迟到完成也必须被丢弃
                            }, FAST))),
                            new FlowBranch("one", List.of(new TaskNode("b1", (ctx, input) -> {
                                failProceed.await(10, TimeUnit.SECONDS);
                                throw new NonRetryableException("branch 1 exploded");
                            }, FAST))),
                            new FlowBranch("two", List.of(new TaskNode("b2", (ctx, input) -> {
                                slowEntered.countDown();
                                awaitIgnoringInterrupts(release, b2Interrupted);
                                return "late-b2";
                            }, FAST)))),
                            (ctx, outputs) -> {
                                joinCalls.incrementAndGet();
                                return String.join("|", outputs);
                            })));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("fanFail", "key-fail", "in");
            assertTrue(slowEntered.await(5, TimeUnit.SECONDS), "两路慢任务都应已进入处理体");
            failProceed.countDown(); // 此时再让第三路失败
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.FAILED, terminal.getStatus(), "一路彻底失败必须落入确定终态");
            assertTrue(b0Interrupted.get() || b2Interrupted.get(), "其他在跑的路应收到取消信号");

            assertEquals(StepStatus.FAILED, findRecord(terminal, "fan[1].b1").getStatus());
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "fan[0].b0").getStatus());
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "fan[2].b2").getStatus());
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "fan.join").getStatus());
            assertEquals(0, joinCalls.get(), "有分支失败时不得执行汇合");

            // 放行被卡住的路：迟到的完成通知不得再改终态/计数。
            release.countDown();
            Thread.sleep(400);
            WorkflowInstance after = engine.get(instance.getId()).orElseThrow();
            assertEquals(WorkflowStatus.FAILED, after.getStatus());
            assertEquals(StepStatus.CANCELLED, findRecord(after, "fan[0].b0").getStatus());
            assertEquals(StepStatus.CANCELLED, findRecord(after, "fan[2].b2").getStatus());
            assertNull(findRecord(after, "fan.join").getOutput());
            assertEquals(0, joinCalls.get());
        } finally {
            engine.shutdown();
        }
    }

    private static void awaitIgnoringInterrupts(CountDownLatch release, AtomicBoolean interruptedFlag) {
        while (true) {
            try {
                release.await(2, TimeUnit.SECONDS);
                return;
            } catch (InterruptedException e) {
                interruptedFlag.set(true);
                Thread.currentThread().interrupt();
                // 故意不立即退出，模拟忽略中断的业务代码，验证迟到通知仍无法改状态。
            }
        }
    }

    // ---------------------------------------------------------------- 一路超时

    @Test
    void oneBranchTimingOutMovesInstanceToTimedOutAndCancelsOthers() throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            RetryPolicy onceShortTimeout = new RetryPolicy(1, 20, 50, 120);
            FlowDefinition definition = new FlowDefinition("fanTimeout", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("fast", List.of(new TaskNode("ok", (ctx, input) -> "OK", FAST))),
                            new FlowBranch("slow", List.of(new TaskNode("slow", (ctx, input) -> {
                                slowEntered.countDown();
                                Thread.sleep(5_000);
                                return "SLOW";
                            }, onceShortTimeout)))),
                            (ctx, outputs) -> String.join("|", outputs))));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("fanTimeout", "key-timeout", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.TIMED_OUT, terminal.getStatus(), "一路超时耗尽必须 TIMED_OUT");
            // 已正常完成的兄弟分支保留 COMPLETED（其结果真实有效），尚未执行的汇合被取消。
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan[0].ok").getStatus());
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "fan.join").getStatus());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 扇出上限

    @Test
    void parallelBranchCountOverPerInstanceCapIsRejectedBeforeAnyExecution() {
        AtomicInteger calls = new AtomicInteger();
        WorkflowEngine capped = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 2);
        try {
            // 分支上限之上还放了一个前置任务：即便如此，超限也必须在流程真正开跑之前被拒绝，
            // 前置任务绝不能先执行、产生副作用。
            FlowDefinition definition = new FlowDefinition("tooWide", List.of(
                    new TaskNode("beforeFan", (ctx, in) -> {
                        calls.incrementAndGet();
                        return "before";
                    }, FAST),
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(new TaskNode("a1", (ctx, in) -> {
                                calls.incrementAndGet();
                                return "a";
                            }, FAST))),
                            new FlowBranch("b", List.of(new TaskNode("b1", (ctx, in) -> {
                                calls.incrementAndGet();
                                return "b";
                            }, FAST))),
                            new FlowBranch("c", List.of(new TaskNode("c1", (ctx, in) -> {
                                calls.incrementAndGet();
                                return "c";
                            }, FAST)))),
                            (ctx, outputs) -> "joined")));
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> capped.registerFlow(definition),
                    "超过扇出上限的定义必须在注册（真正开跑）阶段被明确拒绝");
            assertTrue(error.getMessage().contains("exceeding configured cap 2"));
            assertEquals(0, calls.get(), "被拒绝的流程不得执行任何任务，不产生任何副作用");
        } finally {
            capped.shutdown();
        }
    }

    @Test
    void parallelBranchCountAtCapIsAcceptedAndRuns() {
        AtomicInteger calls = new AtomicInteger();
        WorkflowEngine capped = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 2);
        try {
            FlowDefinition definition = new FlowDefinition("atCap", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(new TaskNode("a1", (ctx, in) -> {
                                calls.incrementAndGet();
                                return "a";
                            }, FAST))),
                            new FlowBranch("b", List.of(new TaskNode("b1", (ctx, in) -> {
                                calls.incrementAndGet();
                                return "b";
                            }, FAST)))),
                            (ctx, outputs) -> "joined")));
            capped.registerFlow(definition); // 恰好等于上限：合法
            WorkflowInstance instance = capped.start("atCap", "key-cap-ok", "in");
            WorkflowInstance terminal = capped.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(2, calls.get());
        } finally {
            capped.shutdown();
        }
    }

    // ---------------------------------------------------------------- 分支内重试

    @Test
    void retryableBranchFailureIsRetriedAndJoinsExactlyOnce() {
        AtomicInteger b0Calls = new AtomicInteger();
        AtomicInteger joinCalls = new AtomicInteger();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("fanRetry", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("flaky", List.of(new TaskNode("b0", (ctx, input) -> {
                                int n = b0Calls.incrementAndGet();
                                if (n < 3) throw new RetryableException("transient #" + n);
                                return "B0";
                            }, FAST))),
                            new FlowBranch("ok", List.of(new TaskNode("b1", (ctx, input) -> "B1", FAST)))),
                            orderedJoin(joinCalls))));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("fanRetry", "key-fr", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(3, b0Calls.get(), "并行分支沿用退避重试语义");
            assertEquals(1, joinCalls.get(), "重试成功后汇合仍只执行一次");
            assertEquals("B0|B1", findRecord(terminal, "fan.join").getOutput());
            assertEquals(3, findRecord(terminal, "fan[0].b0").getAttempt());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 并行期间终止

    @Test
    void terminateDuringParallelCancelsAllBranches() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger joinCalls = new AtomicInteger();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("fanTerm", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(new TaskNode("a1", (ctx, input) -> {
                                entered.countDown();
                                release.await(10, TimeUnit.SECONDS);
                                return "a";
                            }, FAST))),
                            new FlowBranch("b", List.of(new TaskNode("b1", (ctx, input) -> {
                                entered.countDown();
                                release.await(10, TimeUnit.SECONDS);
                                return "b";
                            }, FAST)))),
                            (ctx, outputs) -> {
                                joinCalls.incrementAndGet();
                                return String.join("|", outputs);
                            }),
                    new TaskNode("after", (ctx, input) -> "after", FAST)));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("fanTerm", "key-term", "in");
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTrue(engine.terminate(instance.getId()));
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.TERMINATED, terminal.getStatus());
            assertEquals(0, joinCalls.get());
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "after").getStatus(),
                    "汇合之后的步骤不得执行");
            release.countDown();
            Thread.sleep(200);
            assertEquals(WorkflowStatus.TERMINATED, engine.get(instance.getId()).orElseThrow().getStatus());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 多实例隔离

    @Test
    void manyParallelInstancesStayIsolatedAndEachJoinsOnce() {
        int instanceCount = 8;
        Map<String, Map<String, AtomicInteger>> perInstance = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> joinPerInstance = new ConcurrentHashMap<>();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("multiFan", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(new TaskNode("a1", (ctx, input) -> {
                                perInstance.computeIfAbsent(ctx.instanceId(), k -> new ConcurrentHashMap<>())
                                        .computeIfAbsent("a1", k2 -> new AtomicInteger()).incrementAndGet();
                                Thread.sleep((ctx.instanceId().hashCode() & 0x7fffffff) % 40);
                                return input + ":A";
                            }, FAST))),
                            new FlowBranch("b", List.of(new TaskNode("b1", (ctx, input) -> {
                                perInstance.computeIfAbsent(ctx.instanceId(), k -> new ConcurrentHashMap<>())
                                        .computeIfAbsent("b1", k2 -> new AtomicInteger()).incrementAndGet();
                                Thread.sleep((ctx.instanceId().hashCode() & 0x7fffffff) % 20);
                                return input + ":B";
                            }, FAST)))),
                            (ctx, outputs) -> {
                                joinPerInstance.computeIfAbsent(ctx.instanceId(), k -> new AtomicInteger()).incrementAndGet();
                                return String.join("|", outputs);
                            })));
            engine.registerFlow(definition);

            List<WorkflowInstance> instances = new ArrayList<>();
            for (int i = 0; i < instanceCount; i++) {
                instances.add(engine.start("multiFan", "multi-" + i, "input-" + i));
            }
            for (int i = 0; i < instances.size(); i++) {
                WorkflowInstance instance = instances.get(i);
                WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 15_000);
                assertNotNull(terminal, "instance " + instance.getId() + " 应进入终态");
                assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
                assertEquals("input-" + i + ":A|input-" + i + ":B",
                        findRecord(terminal, "fan.join").getOutput(), "实例间并行结果不得串扰");
                Map<String, AtomicInteger> calls = perInstance.get(instance.getId());
                assertEquals(1, calls.get("a1").get());
                assertEquals(1, calls.get("b1").get());
                assertEquals(1, joinPerInstance.get(instance.getId()).get());
            }
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 分支内多任务顺序推进

    @Test
    void multiTaskBranchesAdvanceInOrderWhileBranchesRunInParallel() {
        AtomicInteger a1Calls = new AtomicInteger();
        AtomicInteger a2Calls = new AtomicInteger();
        AtomicInteger b1Calls = new AtomicInteger();
        AtomicInteger joinCalls = new AtomicInteger();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            TaskNode a1 = new TaskNode("a1", (ctx, input) -> {
                a1Calls.incrementAndGet();
                Thread.sleep(60); // 让 b 路先完成，验证 a 路内部仍严格顺序
                return "A1";
            }, FAST);
            TaskNode a2 = new TaskNode("a2", (ctx, input) -> {
                assertEquals("A1", input, "同分支内后一个任务必须拿到前一个任务的输出");
                a2Calls.incrementAndGet();
                return "A2";
            }, FAST);
            TaskNode b1 = new TaskNode("b1", (ctx, input) -> {
                b1Calls.incrementAndGet();
                return "B1";
            }, FAST);
            FlowDefinition definition = new FlowDefinition("chainFan", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(a1, a2)),
                            new FlowBranch("b", List.of(b1))),
                            (ctx, outputs) -> {
                                joinCalls.incrementAndGet();
                                return String.join("|", outputs);
                            })));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("chainFan", "key-chain", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(1, a1Calls.get());
            assertEquals(1, a2Calls.get());
            assertEquals(1, b1Calls.get());
            assertEquals(1, joinCalls.get());
            assertEquals("A1", findRecord(terminal, "fan[0].a1").getOutput());
            assertEquals("A1", findRecord(terminal, "fan[0].a2").getInput());
            assertEquals("A2|B1", findRecord(terminal, "fan.join").getOutput());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合点崩溃

    @Test
    void crashAfterAllBranchesCompletedStillJoinsExactlyOnceAcrossRestart() throws Exception {
        AtomicInteger b0Calls = new AtomicInteger();
        AtomicInteger b1Calls = new AtomicInteger();
        AtomicInteger joinCalls = new AtomicInteger();
        CountDownLatch b0Done = new CountDownLatch(1);
        CountDownLatch b1Done = new CountDownLatch(1);

        // 第一次运行：两路业务体都执行后阻塞在 latch 上，完成通知尚未入队即杀引擎。
        WorkflowEngine engine1 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine1.registerFlow(joinCrashDefinition(b0Calls, b1Calls, joinCalls, b0Done, b1Done, true));
        WorkflowInstance started = engine1.start("joinCrash", "key-joincrash", "in");
        assertTrue(b0Done.await(5, TimeUnit.SECONDS));
        assertTrue(b1Done.await(5, TimeUnit.SECONDS));
        engine1.shutdown(); // 通知未处理即崩溃：两路记录仍是 RUNNING

        WorkflowEngine engine2 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            engine2.registerFlow(joinCrashDefinition(b0Calls, b1Calls, joinCalls,
                    new CountDownLatch(0), new CountDownLatch(0), false));
            assertEquals(1, engine2.recover());
            WorkflowInstance terminal = engine2.awaitTerminal(started.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            // 崩溃时完成通知未持久化 => 两路各补跑一次（at-least-once，业务幂等是防线），但汇合只一次。
            assertEquals(2, b0Calls.get());
            assertEquals(2, b1Calls.get());
            assertEquals(1, joinCalls.get(), "汇合在恢复后只执行一次");
            assertEquals("B0|B1", findRecord(terminal, "fan.join").getOutput());
        } finally {
            engine2.shutdown();
        }
    }

    /** 汇合点崩溃测试用工作流：block=true 时任务体执行后阻塞（靠 shutdown 中断退出）。 */
    private static FlowDefinition joinCrashDefinition(AtomicInteger b0Calls, AtomicInteger b1Calls,
                                                      AtomicInteger joinCalls,
                                                      CountDownLatch b0Done, CountDownLatch b1Done,
                                                      boolean block) {
        TaskNode b0 = new TaskNode("b0", (ctx, input) -> {
            b0Calls.incrementAndGet();
            b0Done.countDown();
            if (block) {
                try {
                    new CountDownLatch(1).await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
            return "B0";
        }, FAST);
        TaskNode b1 = new TaskNode("b1", (ctx, input) -> {
            b1Calls.incrementAndGet();
            b1Done.countDown();
            if (block) {
                try {
                    new CountDownLatch(1).await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
            return "B1";
        }, FAST);
        return new FlowDefinition("joinCrash", List.of(
                new ParallelNode("fan", List.of(
                        new FlowBranch("zero", List.of(b0)),
                        new FlowBranch("one", List.of(b1))),
                        (ctx, outputs) -> {
                            joinCalls.incrementAndGet();
                            return String.join("|", outputs);
                        })));
    }
}
