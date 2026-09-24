package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.ErrorKind;
import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.NonRetryableException;
import com.github.lisama569.durablefunctionruntimejava.engine.RetryableException;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import org.junit.jupiter.api.AfterEach;
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

/** 重试、超时与终止语义：可重试/不可重试需可区分，终态确定。 */
class WorkflowEngineRetryTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngineRetryTest.class);
    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;

    private WorkflowEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) engine.shutdown();
    }

    private WorkflowEngine newEngine() {
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        return engine;
    }

    @Test
    void retryableFailureIsRetriedAndEventuallySucceeds() {
        AtomicInteger calls = new AtomicInteger();
        newEngine().register(new WorkflowDefinition("flaky", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    int n = calls.incrementAndGet();
                    log.info("[instance={}] step=step1 attempt={} call#{}", ctx.instanceId(), ctx.attempt(), n);
                    if (n < 3) throw new RetryableException("transient error #" + n);
                    return "ok";
                }, FAST))));

        WorkflowInstance instance = engine.start("flaky", "key-retry", "in");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals(3, calls.get(), "前两次失败应触发重试，第三次成功");
        assertEquals(3, terminal.getSteps().get(0).getAttempt());
        assertEquals("ok", terminal.getSteps().get(0).getOutput());
    }

    @Test
    void retryableFailureExhaustsToFailedTerminal() {
        AtomicInteger calls = new AtomicInteger();
        newEngine().register(new WorkflowDefinition("alwaysFail", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    log.info("[instance={}] step=step1 attempt={}", ctx.instanceId(), ctx.attempt());
                    calls.incrementAndGet();
                    throw new RetryableException("always transient");
                }, FAST))));

        WorkflowInstance instance = engine.start("alwaysFail", "key-exhaust", "in");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.FAILED, terminal.getStatus(), "重试耗尽必须落入 FAILED 终态");
        assertEquals(3, calls.get(), "尝试次数不得超过 maxAttempts");
        assertEquals(ErrorKind.RETRYABLE, terminal.getSteps().get(0).getErrorKind());
    }

    @Test
    void nonRetryableFailureFailsImmediatelyWithoutRetry() {
        AtomicInteger calls = new AtomicInteger();
        newEngine().register(new WorkflowDefinition("fatal", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    calls.incrementAndGet();
                    throw new NonRetryableException("business rule violated");
                }, FAST))));

        WorkflowInstance instance = engine.start("fatal", "key-fatal", "in");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.FAILED, terminal.getStatus());
        assertEquals(1, calls.get(), "不可重试错误不得重试");
        assertEquals(ErrorKind.NON_RETRYABLE, terminal.getSteps().get(0).getErrorKind());
    }

    @Test
    void stepTimeoutExhaustsToTimedOutTerminalAndLateCompletionIgnored() throws Exception {
        CountDownLatch stepEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RetryPolicy policy = new RetryPolicy(2, 20, 50, 150);
        newEngine().register(new WorkflowDefinition("slow", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    stepEntered.countDown();
                    log.info("[instance={}] step=step1 attempt={} blocking", ctx.instanceId(), ctx.attempt());
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    return "late-output";
                }, policy))));

        WorkflowInstance instance = engine.start("slow", "key-timeout", "in");
        assertTrue(stepEntered.await(5, TimeUnit.SECONDS));
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.TIMED_OUT, terminal.getStatus(), "超时耗尽必须落入 TIMED_OUT 终态");
        assertEquals(ErrorKind.TIMEOUT, terminal.getSteps().get(0).getErrorKind());

        // 释放被阻塞的 worker，其迟到的完成通知不得改变终态。
        release.countDown();
        Thread.sleep(300);
        WorkflowInstance after = engine.get(instance.getId()).orElseThrow();
        assertEquals(WorkflowStatus.TIMED_OUT, after.getStatus(), "迟到的完成通知不得使终态回退");
    }

    @Test
    void terminateMovesInstanceToTerminalAndSkipsRemainingSteps() throws Exception {
        CountDownLatch step1Running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger step2Calls = new AtomicInteger();
        newEngine().register(new WorkflowDefinition("twoStep", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    step1Running.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    return "s1";
                }, FAST),
                new StepDefinition("step2", (ctx, input) -> {
                    step2Calls.incrementAndGet();
                    return "s2";
                }, FAST))));

        WorkflowInstance instance = engine.start("twoStep", "key-term", "in");
        assertTrue(step1Running.await(5, TimeUnit.SECONDS));
        assertTrue(engine.terminate(instance.getId()));

        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.TERMINATED, terminal.getStatus());
        release.countDown();
        Thread.sleep(300);
        assertEquals(0, step2Calls.get(), "终止后后续步骤不得执行");
        assertEquals(WorkflowStatus.TERMINATED, engine.get(instance.getId()).orElseThrow().getStatus());
    }
}
