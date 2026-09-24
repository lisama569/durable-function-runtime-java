package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 重启恢复：崩溃后已完成步骤不得重复执行，实例从中断点继续，状态不回退。 */
class WorkflowEngineRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngineRecoveryTest.class);
    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;

    @Test
    void crashedInstanceResumesFromInterruptedStepWithoutReexecutingCompletedSteps() throws Exception {
        AtomicInteger step1Calls = new AtomicInteger();
        AtomicInteger step2Calls = new AtomicInteger();
        CountDownLatch step2Entered = new CountDownLatch(1);
        CountDownLatch crashNow = new CountDownLatch(1);

        // 第一次运行：step1 完成，step2 进入后模拟进程崩溃（不完成任务、不推进状态）。
        WorkflowEngine engine1 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine1.register(new WorkflowDefinition("order", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    step1Calls.incrementAndGet();
                    log.info("[instance={}] step=step1 attempt={} executed", ctx.instanceId(), ctx.attempt());
                    return "r1";
                }, FAST),
                new StepDefinition("step2", (ctx, input) -> {
                    step2Calls.incrementAndGet();
                    step2Entered.countDown();
                    log.info("[instance={}] step=step2 attempt={} entered, waiting for crash", ctx.instanceId(), ctx.attempt());
                    try {
                        crashNow.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    return "r2";
                }, FAST))));

        WorkflowInstance started = engine1.start("order", "key-recover", "in");
        assertTrue(step2Entered.await(5, TimeUnit.SECONDS), "step2 应已进入执行");
        // 模拟崩溃：直接杀掉引擎（中断 worker），不等待优雅完成。
        engine1.shutdown();
        assertEquals(1, step1Calls.get());
        assertEquals(1, step2Calls.get());

        // 重启：新引擎加载同一数据目录，恢复后应跳过已完成的 step1，从 step2 继续。
        WorkflowEngine engine2 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            engine2.register(new WorkflowDefinition("order", List.of(
                    new StepDefinition("step1", (ctx, input) -> {
                        step1Calls.incrementAndGet();
                        return "r1";
                    }, FAST),
                    new StepDefinition("step2", (ctx, input) -> {
                        step2Calls.incrementAndGet();
                        log.info("[instance={}] step=step2 attempt={} resumed after restart", ctx.instanceId(), ctx.attempt());
                        return "r2";
                    }, FAST))));

            int recovered = engine2.recover();
            assertEquals(1, recovered, "应恢复 1 个未完结实例");

            WorkflowInstance terminal = engine2.awaitTerminal(started.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(1, step1Calls.get(), "已完成步骤重启后不得重复执行");
            assertEquals(2, step2Calls.get(), "中断步骤应被重试一次后完成");
            assertEquals("r1", terminal.getSteps().get(0).getOutput(), "已完成步骤的输出不得丢失");
            assertEquals("r2", terminal.getSteps().get(1).getOutput());
        } finally {
            engine2.shutdown();
        }
    }

    @Test
    void completedInstanceIsNotResumedOrReexecutedAfterRestart() {
        AtomicInteger calls = new AtomicInteger();
        WorkflowEngine engine1 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine1.register(new WorkflowDefinition("done", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    calls.incrementAndGet();
                    return "r1";
                }, FAST))));
        WorkflowInstance instance = engine1.start("done", "key-done", "in");
        assertNotNull(engine1.awaitTerminal(instance.getId(), 10_000));
        assertEquals(1, calls.get());
        engine1.shutdown();

        WorkflowEngine engine2 = new WorkflowEngine(new FileWorkflowStore(dataDir));
        try {
            engine2.register(new WorkflowDefinition("done", List.of(
                    new StepDefinition("step1", (ctx, input) -> {
                        calls.incrementAndGet();
                        return "r1";
                    }, FAST))));
            assertEquals(0, engine2.recover(), "已完结实例不应被恢复");
            assertEquals(WorkflowStatus.COMPLETED, engine2.get(instance.getId()).orElseThrow().getStatus());
            assertEquals(1, calls.get(), "已完结实例重启后不得再次执行");
        } finally {
            engine2.shutdown();
        }
    }
}
