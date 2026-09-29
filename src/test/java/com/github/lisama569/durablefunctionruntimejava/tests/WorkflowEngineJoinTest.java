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

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 汇合（JoinHandler）专项语义：
 * - 汇合在 worker 线程执行，单个实例的汇合再慢/阻塞也不拖住其他实例；
 * - 汇合沿用与普通任务一致的退避重试、超时与失败语义；
 * - 汇合彻底失败/超时后实例进入确定终态，且不留下后台任务继续改状态。
 */
class WorkflowEngineJoinTest {

    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);
    /** 汇合专用策略：1 次重试、极短退避、很短超时，便于确定性地复现重试/超时。 */
    private static final RetryPolicy JOIN_FAST = new RetryPolicy(2, 20, 50, 300);
    private static final RetryPolicy JOIN_NO_RETRY_SHORT_TIMEOUT = new RetryPolicy(1, 20, 50, 200);

    @TempDir
    Path dataDir;

    private static StepRecord findRecord(WorkflowInstance instance, String path) {
        return instance.getSteps().stream().filter(s -> path.equals(s.getPath())).findFirst().orElseThrow();
    }

    private static TaskNode task(String name, com.github.lisama569.durablefunctionruntimejava.workflow.StepHandler handler,
                                 RetryPolicy policy) {
        return new TaskNode(name, handler, policy);
    }

    /** 两个单任务分支的并行节点，汇合策略可配。 */
    private static ParallelNode parallel(String node, RetryPolicy joinPolicy,
                                         com.github.lisama569.durablefunctionruntimejava.workflow.JoinHandler join) {
        return new ParallelNode(node, List.of(
                new FlowBranch("a", List.of(task("a", (ctx, in) -> "A", FAST))),
                new FlowBranch("b", List.of(task("b", (ctx, in) -> "B", FAST)))),
                join, joinPolicy);
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

    // ---------------------------------------------------------------- 阻塞汇合不隔离

    /**
     * 一个实例的汇合永久阻塞：其他多个实例（含串行与并行）仍应照常推进到 COMPLETED，
     * 各自汇合照常执行。最后终止被卡住的实例，验证它能脱离阻塞进入终态。
     */
    @Test
    void blockedJoinInOneInstanceDoesNotStallOtherInstances() throws Exception {
        // 单 worker 时若汇合内联在事件循环，会直接卡死整个引擎：
        // 这里特意只给 2 个 worker，证明汇合执行已移出事件循环且有独立执行位。
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 2, 8);
        try {
            CountDownLatch blockingJoinEntered = new CountDownLatch(1);
            CountDownLatch neverRelease = new CountDownLatch(1);
            engine.registerFlow(new FlowDefinition("stuckFlow", List.of(
                    parallel("fan", new RetryPolicy(5, 100, 1_000, 60_000), (ctx, outputs) -> {
                        blockingJoinEntered.countDown();
                        neverRelease.await(); // 模拟汇合永久阻塞
                        return "never";
                    }))));

            AtomicInteger healthyJoinCalls = new AtomicInteger();
            engine.registerFlow(new FlowDefinition("healthyFlow", List.of(
                    parallel("fan", FAST, (ctx, outputs) -> {
                        healthyJoinCalls.incrementAndGet();
                        return String.join("|", outputs);
                    }),
                    task("after", (ctx, in) -> "after:" + in, FAST))));

            WorkflowInstance stuck = engine.start("stuckFlow", "stuck-1", "in");
            assertTrue(blockingJoinEntered.await(5, TimeUnit.SECONDS), "卡住实例的汇合应已在 worker 中开始");
            assertTrue(waitRecord(engine, stuck.getId(), "fan.join", StepStatus.RUNNING, 5_000),
                    "卡住实例的汇合保持 RUNNING");

            // 卡住期间连续启动多个健康实例：它们都必须独立推进到终态。
            for (int i = 0; i < 3; i++) {
                WorkflowInstance healthy = engine.start("healthyFlow", "healthy-" + i, "in" + i);
                WorkflowInstance terminal = engine.awaitTerminal(healthy.getId(), 10_000);
                assertNotNull(terminal, "健康实例 " + i + " 不应被卡住的汇合拖住");
                assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
                assertEquals("A|B", findRecord(terminal, "fan.join").getOutput());
                assertEquals("after:A|B", findRecord(terminal, "after").getOutput());
            }
            assertEquals(3, healthyJoinCalls.get(), "健康实例的汇合各自照常执行一次");
            assertEquals(StepStatus.RUNNING, findRecord(engine.get(stuck.getId()).orElseThrow(), "fan.join").getStatus());

            // 终止仍能打断被阻塞的汇合（future.cancel 中断等待），实例进入确定终态。
            assertTrue(engine.terminate(stuck.getId()));
            WorkflowInstance stuckTerminal = engine.awaitTerminal(stuck.getId(), 5_000);
            assertNotNull(stuckTerminal);
            assertEquals(WorkflowStatus.TERMINATED, stuckTerminal.getStatus());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合可重试错误退避重试到成功

    /**
     * 汇合前两次抛 RetryableException、第三次成功：实例最终 COMPLETED，汇合记录 attempt=3，
     * 输出为最后一次结果；分支任务只执行一次（重试只重跑汇合，不回滚/重跑分支）。
     */
    @Test
    void retryableJoinFailureIsRetriedWithBackoffAndEventuallySucceeds() {
        AtomicInteger joinCalls = new AtomicInteger();
        AtomicInteger branchCalls = new AtomicInteger();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            ParallelNode node = new ParallelNode("fan", List.of(
                    new FlowBranch("a", List.of(task("a", (ctx, in) -> {
                        branchCalls.incrementAndGet();
                        return "A";
                    }, FAST))),
                    new FlowBranch("b", List.of(task("b", (ctx, in) -> {
                        branchCalls.incrementAndGet();
                        return "B";
                    }, FAST)))),
                    (ctx, outputs) -> {
                        int n = joinCalls.incrementAndGet();
                        if (n < 3) throw new RetryableException("transient join #" + n);
                        return "joined(" + String.join(",", outputs) + ")";
                    },
                    new RetryPolicy(4, 30, 200, 5_000));
            engine.registerFlow(new FlowDefinition("joinRetry", List.of(node)));

            WorkflowInstance instance = engine.start("joinRetry", "key-jr", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(3, joinCalls.get(), "可重试的汇合错误必须按退避重试直到成功");
            assertEquals(3, findRecord(terminal, "fan.join").getAttempt(), "汇合 attempt 计数反映重试次数");
            assertEquals("joined(A,B)", findRecord(terminal, "fan.join").getOutput());
            assertEquals(2, branchCalls.get(), "汇合重试不得导致分支任务重跑");
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合不可恢复失败 -> 确定终态且无后台任务

    /**
     * 汇合抛 NonRetryableException：实例立即 FAILED，汇合记录 FAILED，后续步骤不执行；
     * 即使业务侧随后放行（模拟被取消的后台工作晚到），也不会再次执行汇合或改写终态。
     */
    @Test
    void nonRetryableJoinFailureFailsInstanceWithoutBackgroundRejoins() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        CountDownLatch joinEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("joinFatal", List.of(
                    parallel("fan", JOIN_NO_RETRY_SHORT_TIMEOUT, (ctx, outputs) -> {
                        joinCalls.incrementAndGet();
                        joinEntered.countDown();
                        // 先到达汇合点，再由测试放行后立刻抛不可重试错误。
                        release.await(5, TimeUnit.SECONDS);
                        throw new NonRetryableException("join permanently broken");
                    }),
                    task("after", (ctx, in) -> "after", FAST)));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("joinFatal", "key-jf", "in");
            assertTrue(joinEntered.await(5, TimeUnit.SECONDS));
            release.countDown();

            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.FAILED, terminal.getStatus(), "汇合不可恢复错误必须使实例 FAILED");
            assertEquals(StepStatus.FAILED, findRecord(terminal, "fan.join").getStatus());
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan[0].a").getStatus(),
                    "已完成分支结果真实有效，保留 COMPLETED");
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "after").getStatus(), "汇合失败后的步骤不得执行");
            assertEquals(1, joinCalls.get(), "不可重试错误不产生后台重试");

            // 等待超过任何可能的调度窗口，确认没有遗留后台任务再次触发汇合或改写状态。
            Thread.sleep(400);
            WorkflowInstance later = engine.get(instance.getId()).orElseThrow();
            assertEquals(WorkflowStatus.FAILED, later.getStatus());
            assertEquals(1, joinCalls.get(), "终态后不得再有后台汇合执行");
            assertEquals(StepStatus.FAILED, findRecord(later, "fan.join").getStatus());
        } finally {
            engine.shutdown();
        }
    }

    /**
     * 汇合始终抛 RetryableException：达到 maxAttempts 后实例 FAILED（重试耗尽），
     * 汇合恰好被尝试 maxAttempts 次，之后不再有任何后台重试。
     */
    @Test
    void joinRetryExhaustionMovesInstanceToFailedAndStopsRetries() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            int maxAttempts = 3;
            RetryPolicy joinPolicy = new RetryPolicy(maxAttempts, 20, 50, 5_000);
            engine.registerFlow(new FlowDefinition("joinRetryExhaust", List.of(
                    parallel("fan", joinPolicy, (ctx, outputs) -> {
                        joinCalls.incrementAndGet();
                        throw new RetryableException("join always transient");
                    }))));

            WorkflowInstance instance = engine.start("joinRetryExhaust", "key-jre", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.FAILED, terminal.getStatus(), "重试耗尽必须落入确定终态");
            assertEquals(maxAttempts, joinCalls.get(), "汇合最多尝试 maxAttempts 次");
            assertEquals(maxAttempts, findRecord(terminal, "fan.join").getAttempt());

            Thread.sleep(400); // 超过退避窗口，确认没有遗留的后台重试。
            assertEquals(maxAttempts, joinCalls.get());
            assertEquals(WorkflowStatus.FAILED, engine.get(instance.getId()).orElseThrow().getStatus());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合超时 -> TIMED_OUT 且取消阻塞中的汇合

    /**
     * 汇合长时间不返回：超过 stepTimeoutMillis 后被看门狗取消（收到中断），
     * maxAttempts=1 时实例 TIMED_OUT；阻塞中的汇合后台任务被取消，不会有迟到的成功改写终态。
     */
    @Test
    void joinBlockingBeyondTimeoutMovesInstanceToTimedOutAndCancelsJoin() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch joinEntered = new CountDownLatch(1);
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            engine.registerFlow(new FlowDefinition("joinTimeout", List.of(
                    parallel("fan", JOIN_NO_RETRY_SHORT_TIMEOUT, (ctx, outputs) -> {
                        joinCalls.incrementAndGet();
                        joinEntered.countDown();
                        try {
                            Thread.sleep(5_000); // 远超 200ms 超时
                        } catch (InterruptedException e) {
                            interrupted.set(true);
                            Thread.currentThread().interrupt();
                            throw e; // 响应取消：不投递成功通知
                        }
                        return "late-join";
                    }),
                    task("after", (ctx, in) -> "after", FAST))));

            WorkflowInstance instance = engine.start("joinTimeout", "key-jt", "in");
            assertTrue(joinEntered.await(5, TimeUnit.SECONDS));
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.TIMED_OUT, terminal.getStatus(), "汇合超时耗尽必须 TIMED_OUT");
            assertEquals(StepStatus.FAILED, findRecord(terminal, "fan.join").getStatus());
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "after").getStatus());
            assertEquals(1, joinCalls.get(), "maxAttempts=1 时超时不重试");
            assertTrue(interrupted.get(), "超时应中断阻塞中的汇合");

            // 等到被取消的汇合早已越过其 sleep 上限（5s 之内被中断），确认没有迟到成功改写终态。
            Thread.sleep(300);
            WorkflowInstance later = engine.get(instance.getId()).orElseThrow();
            assertEquals(WorkflowStatus.TIMED_OUT, later.getStatus());
            assertNull(findRecord(later, "fan.join").getOutput());
            assertEquals(1, joinCalls.get());
        } finally {
            engine.shutdown();
        }
    }

    /** 汇合第一次超时、第二次在时限内成功：超时按可重试处理，最终 COMPLETED，attempt=2。 */
    @Test
    void joinTimingOutOnceIsRetriedAndThenSucceeds() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            // 3 次尝试、200ms 超时、极短退避。
            RetryPolicy joinPolicy = new RetryPolicy(3, 10, 50, 200);
            engine.registerFlow(new FlowDefinition("joinTimeoutRetry", List.of(
                    parallel("fan", joinPolicy, (ctx, outputs) -> {
                        int n = joinCalls.incrementAndGet();
                        if (n == 1) {
                            try {
                                Thread.sleep(2_000); // 第一次卡住直到超时中断
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw e;
                            }
                        }
                        return "joined";
                    }))));

            WorkflowInstance instance = engine.start("joinTimeoutRetry", "key-jtr", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(2, joinCalls.get(), "第一次超时后应重试，第二次成功");
            assertEquals(2, findRecord(terminal, "fan.join").getAttempt());
            assertEquals("joined", findRecord(terminal, "fan.join").getOutput());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合在跑时崩溃 -> 恢复后补汇合一次

    /**
     * 所有分支已完成、汇合正在 worker 中阻塞时引擎被杀：重启 recover() 后把在跑的汇合退回 PENDING
     * 并重新派发，最终 COMPLETED，汇合恰好成功一次（崩溃时的旧汇合被中断，其迟到通知被丢弃）。
     */
    @Test
    void crashWhileJoinRunningRejoinsExactlyOnceAfterRecovery() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        CountDownLatch joinEntered = new CountDownLatch(1);

        WorkflowEngine engine1 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine1.registerFlow(new FlowDefinition("joinCrashRun", List.of(
                parallel("fan", FAST, (ctx, outputs) -> {
                    joinCalls.incrementAndGet();
                    joinEntered.countDown();
                    try {
                        new CountDownLatch(1).await(10, TimeUnit.SECONDS); // 靠 shutdown 中断退出
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    return "stale";
                }))));
        WorkflowInstance started = engine1.start("joinCrashRun", "key-jcrash", "in");
        assertTrue(joinEntered.await(5, TimeUnit.SECONDS));
        assertTrue(waitRecord(engine1, started.getId(), "fan.join", StepStatus.RUNNING, 5_000));
        engine1.shutdown();

        WorkflowEngine engine2 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            engine2.registerFlow(new FlowDefinition("joinCrashRun", List.of(
                    parallel("fan", FAST, (ctx, outputs) -> {
                        joinCalls.incrementAndGet();
                        return String.join("|", outputs);
                    }))));
            assertEquals(1, engine2.recover());
            WorkflowInstance terminal = engine2.awaitTerminal(started.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(2, joinCalls.get(), "崩溃时在跑的汇合按 at-least-once 补跑一次");
            assertEquals("A|B", findRecord(terminal, "fan.join").getOutput());
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan[0].a").getStatus());
        } finally {
            engine2.shutdown();
        }
    }
}
