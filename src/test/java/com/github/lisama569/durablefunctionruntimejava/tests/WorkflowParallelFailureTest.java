package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.ErrorKind;
import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.NonRetryableException;
import com.github.lisama569.durablefunctionruntimejava.engine.RetryableException;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.ControlFlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ActivityNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并行失败语义：任一路彻底失败/超时 -> 确定终态，并取消其他在跑分支，不留后台任务改状态。 */
class WorkflowParallelFailureTest {

    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;
    private WorkflowEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) engine.shutdown();
    }

    private StepRecord step(WorkflowInstance instance, String suffix) {
        return instance.getSteps().stream()
                .filter(s -> s.getNodeId().endsWith(suffix))
                .findFirst().orElseThrow(() -> new AssertionError("missing " + suffix));
    }

    @Test
    void oneBranchFailsNonRetryable_instanceFailsAndOtherBranchesCancelled() throws Exception {
        AtomicInteger joinCalls = new AtomicInteger();
        AtomicInteger goodAfterInterrupt = new AtomicInteger();
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);

        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        engine.register(ControlFlowDefinition.name("pfail")
                .then(new ParallelNode("work", List.of(
                        new ParallelBranch("bad", "doBad", (ctx, in) -> {
                            throw new NonRetryableException("boom");
                        }, FAST),
                        new ParallelBranch("slow", "doSlow", (ctx, in) -> {
                            slowEntered.countDown();
                            try {
                                releaseSlow.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                goodAfterInterrupt.incrementAndGet();
                                throw e;
                            }
                            return "slow-done";
                        }, FAST)),
                        "merge",
                        (ctx, results) -> {
                            joinCalls.incrementAndGet();
                            return "joined";
                        }, FAST))
                .then(new ActivityNode("after", (ctx, in) -> "after", FAST))
                .build());

        WorkflowInstance started = engine.start("pfail", "key-fail", "in");
        assertTrue(slowEntered.await(5, TimeUnit.SECONDS), "slow 分支应已开始执行");

        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.FAILED, terminal.getStatus(), "不可重试失败必须落入 FAILED");
        assertEquals(ErrorKind.NON_RETRYABLE, step(terminal, "work/p/bad").getErrorKind());
        assertEquals(0, joinCalls.get(), "有分支失败时汇合不得执行");
        // slow 分支应被取消（中断），不能留着后台任务；after 步骤不得运行。
        assertEquals(1, goodAfterInterrupt.get(), "其他在跑分支应被中断取消");
        StepStatus slowStatus = step(terminal, "work/p/slow").getStatus();
        assertEquals(StepStatus.FAILED, slowStatus, "被取消分支状态应为 FAILED");
        assertEquals(StepStatus.PENDING, step(terminal, "after").getStatus(),
                "失败后后续步骤不得执行");

        // 释放阻塞后，迟到结果不得改变终态。
        releaseSlow.countDown();
        Thread.sleep(300);
        assertEquals(WorkflowStatus.FAILED, engine.get(started.getId()).orElseThrow().getStatus(),
                "迟到结果不得改变终态");
    }

    @Test
    void oneBranchExhaustsRetries_instanceFailsWithRetryableKind() {
        AtomicInteger badCalls = new AtomicInteger();
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        engine.register(ControlFlowDefinition.name("pexhaust")
                .then(new ParallelNode("work", List.of(
                        new ParallelBranch("bad", "doBad", (ctx, in) -> {
                            badCalls.incrementAndGet();
                            throw new RetryableException("transient");
                        }, FAST),
                        new ParallelBranch("good", "doGood", (ctx, in) -> "good", FAST)),
                        "merge", (ctx, r) -> "j", FAST))
                .build());

        WorkflowInstance started = engine.start("pexhaust", "key-ex", "in");
        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.FAILED, terminal.getStatus());
        assertEquals(3, badCalls.get(), "重试次数不得超过 maxAttempts");
        assertEquals(ErrorKind.RETRYABLE, step(terminal, "work/p/bad").getErrorKind());
    }

    @Test
    void oneBranchTimesOut_instanceTimesOutAndOthersCancelled() throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger interrupted = new AtomicInteger();
        RetryPolicy timeoutOnce = new RetryPolicy(1, 20, 50, 120);

        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        engine.register(ControlFlowDefinition.name("ptimeout")
                .then(new ParallelNode("work", List.of(
                        new ParallelBranch("slow", "doSlow", (ctx, in) -> {
                            slowEntered.countDown();
                            try {
                                release.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                interrupted.incrementAndGet();
                                throw e;
                            }
                            return "slow";
                        }, timeoutOnce),
                        new ParallelBranch("fast", "doFast", (ctx, in) -> "fast", FAST)),
                        "merge", (ctx, r) -> "j", FAST))
                .build());

        WorkflowInstance started = engine.start("ptimeout", "key-to", "in");
        assertTrue(slowEntered.await(5, TimeUnit.SECONDS));
        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.TIMED_OUT, terminal.getStatus(), "超时耗尽必须落入 TIMED_OUT");
        assertEquals(ErrorKind.TIMEOUT, step(terminal, "work/p/slow").getErrorKind());
        assertEquals(1, interrupted.get(), "超时后该分支 worker 应被中断");
        release.countDown();
    }

    @Test
    void terminateDuringParallelStopsAllBranches() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger interrupted = new AtomicInteger();

        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        engine.register(ControlFlowDefinition.name("pterm")
                .then(new ParallelNode("work", List.of(
                        new ParallelBranch("a", "doA", (ctx, in) -> {
                            entered.countDown();
                            try {
                                release.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                interrupted.incrementAndGet();
                                throw e;
                            }
                            return "a";
                        }, FAST),
                        new ParallelBranch("b", "doB", (ctx, in) -> {
                            entered.countDown();
                            try {
                                release.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                interrupted.incrementAndGet();
                                throw e;
                            }
                            return "b";
                        }, FAST)),
                        "merge", (ctx, r) -> "j", FAST))
                .then(new ActivityNode("after", (ctx, in) -> "after", FAST))
                .build());

        WorkflowInstance started = engine.start("pterm", "key-term", "in");
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertTrue(engine.terminate(started.getId()));

        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.TERMINATED, terminal.getStatus());
        assertEquals(2, interrupted.get(), "终止必须取消所有在跑分支");
        release.countDown();
        Thread.sleep(200);
        assertEquals(WorkflowStatus.TERMINATED, engine.get(started.getId()).orElseThrow().getStatus());
    }
}
