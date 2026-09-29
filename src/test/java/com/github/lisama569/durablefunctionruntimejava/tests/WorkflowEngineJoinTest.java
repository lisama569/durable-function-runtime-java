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
 * 汇合（join）语义专项：
 * <ol>
 *   <li>单个实例的汇合很慢/阻塞时，同进程其他实例仍能独立推进到终态；</li>
 *   <li>汇合抛可重试错误时按退避重试，最终成功；</li>
 *   <li>汇合彻底失败/超时后实例进入确定终态，取消仍在跑的任务，不留后台写入；</li>
 *   <li>超过扇出上限的定义在真正执行前就被拒绝。</li>
 * </ol>
 */
class WorkflowEngineJoinTest {

    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;

    private static StepRecord findRecord(WorkflowInstance instance, String path) {
        return instance.getSteps().stream().filter(s -> path.equals(s.getPath())).findFirst().orElseThrow();
    }

    private static TaskNode echoTask(String name) {
        return new TaskNode(name, (ctx, input) -> name.toUpperCase(), FAST);
    }

    // ---------------------------------------------------------------- 汇合阻塞不影响其他实例

    @Test
    void aSlowOrBlockedJoinDoesNotBlockOtherInstances() throws Exception {
        CountDownLatch stuckJoinEntered = new CountDownLatch(1);
        CountDownLatch releaseStuckJoin = new CountDownLatch(1);
        AtomicInteger healthyJoins = new AtomicInteger();
        // 充足的 worker 线程 + 汇合不在事件循环执行：A 的汇合卡住时，B 的任务/汇合都能照常拿到线程。
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        try {
            FlowDefinition stuckFlow = new FlowDefinition("stuckFan", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(echoTask("a1"))),
                            new FlowBranch("b", List.of(echoTask("b1")))),
                            (ctx, outputs) -> {
                                stuckJoinEntered.countDown();
                                releaseStuckJoin.await(30, TimeUnit.SECONDS);
                                return String.join("|", outputs);
                            })));
            FlowDefinition healthyFlow = new FlowDefinition("healthyFan", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("c", List.of(echoTask("c1"))),
                            new FlowBranch("d", List.of(echoTask("d1")))),
                            (ctx, outputs) -> {
                                healthyJoins.incrementAndGet();
                                return String.join("|", outputs);
                            })));
            engine.registerFlow(stuckFlow);
            engine.registerFlow(healthyFlow);

            WorkflowInstance stuck = engine.start("stuckFan", "key-stuck", "in");
            assertTrue(stuckJoinEntered.await(5, TimeUnit.SECONDS), "卡住的汇合应已开始执行");

            // A 的汇合阻塞期间，连续跑两个健康实例：它们必须独立推进到 COMPLETED。
            for (int i = 0; i < 2; i++) {
                WorkflowInstance healthy = engine.start("healthyFan", "key-healthy-" + i, "in");
                WorkflowInstance terminal = engine.awaitTerminal(healthy.getId(), 10_000);
                assertNotNull(terminal, "其他实例不应被卡住的汇合拖住");
                assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
                assertEquals("C1|D1", findRecord(terminal, "fan.join").getOutput());
            }
            assertEquals(2, healthyJoins.get());

            // A 一直停在 RUNNING（汇合 RUNNING），没有因为别人完成而被推进或判死。
            WorkflowInstance stuckView = engine.get(stuck.getId()).orElseThrow();
            assertEquals(WorkflowStatus.RUNNING, stuckView.getStatus());
            assertEquals(StepStatus.RUNNING, findRecord(stuckView, "fan.join").getStatus());

            // 放行后 A 自己也能正常汇合完成。
            releaseStuckJoin.countDown();
            WorkflowInstance stuckTerminal = engine.awaitTerminal(stuck.getId(), 10_000);
            assertNotNull(stuckTerminal);
            assertEquals(WorkflowStatus.COMPLETED, stuckTerminal.getStatus());
            assertEquals("A1|B1", findRecord(stuckTerminal, "fan.join").getOutput());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合可重试错误最终成功

    @Test
    void retryableJoinFailureIsRetriedWithBackoffAndEventuallySucceeds() {
        AtomicInteger joinCalls = new AtomicInteger();
        RetryPolicy joinRetry = new RetryPolicy(4, 20, 100, 5_000);
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("joinRetry", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(echoTask("a1"))),
                            new FlowBranch("b", List.of(echoTask("b1")))),
                            (ctx, outputs) -> {
                                int n = joinCalls.incrementAndGet();
                                if (n < 3) {
                                    throw new RetryableException("transient join error #" + n);
                                }
                                return String.join("|", outputs);
                            }, joinRetry)));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("joinRetry", "key-jr", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(3, joinCalls.get(), "汇合应按退避重试直到成功");
            StepRecord joinRecord = findRecord(terminal, "fan.join");
            assertEquals(StepStatus.COMPLETED, joinRecord.getStatus());
            assertEquals(3, joinRecord.getAttempt(), "汇合 attempt 应反映重试次数");
            assertEquals("A1|B1", joinRecord.getOutput());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合彻底失败

    @Test
    void nonRetryableJoinFailureFailsInstanceAndCancelsRest() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("joinFatal", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(echoTask("a1"))),
                            new FlowBranch("b", List.of(echoTask("b1")))),
                            (ctx, outputs) -> {
                                joinCalls.incrementAndGet();
                                throw new NonRetryableException("join logic broken");
                            }),
                    // 汇合失败时该任务尚未执行，应被整体取消，不留后台动作。
                    new TaskNode("after", (ctx, input) -> "after", FAST)));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("joinFatal", "key-jf", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.FAILED, terminal.getStatus());
            assertEquals(1, joinCalls.get(), "不可重试错误不得重试");
            StepRecord joinRecord = findRecord(terminal, "fan.join");
            assertEquals(StepStatus.FAILED, joinRecord.getStatus());
            assertEquals(1, joinRecord.getAttempt());
            assertEquals(StepStatus.CANCELLED, findRecord(terminal, "after").getStatus(),
                    "汇合判死后后续任务标记 CANCELLED，不执行");
            // 等一拍确认没有后台任务在汇合失败后再改状态。
            Thread.sleep(200);
            WorkflowInstance again = engine.get(instance.getId()).orElseThrow();
            assertEquals(WorkflowStatus.FAILED, again.getStatus());
            assertEquals(StepStatus.FAILED, findRecord(again, "fan.join").getStatus());
            assertNull(findRecord(again, "after").getOutput());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合超时

    @Test
    void joinThatNeverReturnsTimesOutAndInstanceReachesTerminalStateWithoutBackgroundWrites() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        AtomicBoolean joinInterrupted = new AtomicBoolean();
        CountDownLatch joinEntered = new CountDownLatch(1);
        CountDownLatch releaseJoin = new CountDownLatch(1);
        // 只给一次尝试、很短超时：卡住的汇合必须被判 TIMED_OUT，而不是无限挂着。
        RetryPolicy joinTimeoutOnce = new RetryPolicy(1, 20, 50, 150);
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("joinTimeout", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(echoTask("a1"))),
                            new FlowBranch("b", List.of(echoTask("b1")))),
                            (ctx, outputs) -> {
                                joinCalls.incrementAndGet();
                                joinEntered.countDown();
                                try {
                                    releaseJoin.await(10, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    joinInterrupted.set(true);
                                    Thread.currentThread().interrupt();
                                    throw e;
                                }
                                return String.join("|", outputs);
                            }, joinTimeoutOnce)));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("joinTimeout", "key-jt", "in");
            assertTrue(joinEntered.await(5, TimeUnit.SECONDS));
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal, "汇合一直不返回也必须有超时兜底，实例要能进入终态");
            assertEquals(WorkflowStatus.TIMED_OUT, terminal.getStatus());
            StepRecord joinRecord = findRecord(terminal, "fan.join");
            assertEquals(StepStatus.FAILED, joinRecord.getStatus());
            assertEquals(1, joinRecord.getAttempt());
            assertEquals(com.github.lisama569.durablefunctionruntimejava.domain.ErrorKind.TIMEOUT,
                    joinRecord.getErrorKind());
            // 已完成的兄弟分支结果保留。
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan[0].a1").getStatus());
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan[1].b1").getStatus());

            // 等超时取消真正送达并放行卡住的汇合：迟到的返回不得再写终态。
            assertTrue(poll(() -> joinInterrupted.get(), 5_000), "超时应中断卡住的汇合执行");
            releaseJoin.countDown();
            Thread.sleep(300);
            WorkflowInstance after = engine.get(instance.getId()).orElseThrow();
            assertEquals(WorkflowStatus.TIMED_OUT, after.getStatus());
            assertNull(findRecord(after, "fan.join").getOutput(), "超时后迟到返回不得写入汇合输出");
            assertEquals(1, joinCalls.get(), "只尝试一次（maxAttempts=1），超时后不得重跑汇合");
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合重试耗尽

    @Test
    void joinRetriesExhaustedFailsInstance() {
        AtomicInteger joinCalls = new AtomicInteger();
        RetryPolicy joinRetry = new RetryPolicy(3, 10, 30, 5_000);
        WorkflowEngine engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            FlowDefinition definition = new FlowDefinition("joinRetryExhausted", List.of(
                    new ParallelNode("fan", List.of(
                            new FlowBranch("a", List.of(echoTask("a1"))),
                            new FlowBranch("b", List.of(echoTask("b1")))),
                            (ctx, outputs) -> {
                                joinCalls.incrementAndGet();
                                throw new RetryableException("always transient in join");
                            }, joinRetry)));
            engine.registerFlow(definition);

            WorkflowInstance instance = engine.start("joinRetryExhausted", "key-jre", "in");
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.FAILED, terminal.getStatus());
            assertEquals(3, joinCalls.get(), "可重试错误重试到上限后必须落入确定终态");
            StepRecord joinRecord = findRecord(terminal, "fan.join");
            assertEquals(StepStatus.FAILED, joinRecord.getStatus());
            assertEquals(3, joinRecord.getAttempt());
        } finally {
            engine.shutdown();
        }
    }

    // ---------------------------------------------------------------- 汇合执行中崩溃恢复

    @Test
    void crashWhileJoinRunningReRunsJoinExactlyOnceAcrossRestart() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        CountDownLatch joinEntered = new CountDownLatch(1);

        // 第一次：分支很快完成、汇合进入处理体后阻塞，完成通知尚未持久化即杀引擎。
        WorkflowEngine engine1 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        FlowDefinition blocking = joinCrashDefinition(joinCalls, joinEntered, true);
        engine1.registerFlow(blocking);
        WorkflowInstance started = engine1.start("joinCrashMid", "key-jcm", "in");
        assertTrue(joinEntered.await(5, TimeUnit.SECONDS));
        engine1.shutdown(); // 汇合仍 RUNNING

        WorkflowEngine engine2 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            engine2.registerFlow(joinCrashDefinition(joinCalls, new CountDownLatch(0), false));
            assertEquals(1, engine2.recover());
            WorkflowInstance terminal = engine2.awaitTerminal(started.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            // at-least-once：汇合体被再次调用（业务需幂等），但成功汇合只生效一次、输出正确。
            assertEquals(2, joinCalls.get());
            assertEquals("A1|B1", findRecord(terminal, "fan.join").getOutput());
            assertEquals(StepStatus.COMPLETED, findRecord(terminal, "fan.join").getStatus());
        } finally {
            engine2.shutdown();
        }
    }

    /** 汇合崩溃测试用定义：block=true 时汇合体执行后阻塞（靠 shutdown 中断退出）。 */
    private static FlowDefinition joinCrashDefinition(AtomicInteger joinCalls,
                                                      CountDownLatch joinEntered, boolean block) {
        return new FlowDefinition("joinCrashMid", List.of(
                new ParallelNode("fan", List.of(
                        new FlowBranch("a", List.of(new TaskNode("a1", (ctx, in) -> "A1", FAST))),
                        new FlowBranch("b", List.of(new TaskNode("b1", (ctx, in) -> "B1", FAST)))),
                        (ctx, outputs) -> {
                            joinCalls.incrementAndGet();
                            joinEntered.countDown();
                            if (block) {
                                new CountDownLatch(1).await(10, TimeUnit.SECONDS);
                            }
                            return String.join("|", outputs);
                        })));
    }

    private static boolean poll(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
