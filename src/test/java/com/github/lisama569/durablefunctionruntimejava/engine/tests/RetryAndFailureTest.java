package com.github.lisama569.durablefunctionruntimejava.engine.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.durablefunctionruntimejava.engine.DurableWorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.engine.model.ErrorType;
import com.github.lisama569.durablefunctionruntimejava.engine.model.InstanceStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StartRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepState;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.SideEffectCounters;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.TestHarness;

/**
 * Covers: retryable failures are retried with capped exponential backoff and
 * eventually succeed; non-retryable failures fail immediately with a stable
 * code; exhausted retries settle in a terminal state.
 */
class RetryAndFailureTest {

    private static final Logger log = LoggerFactory.getLogger("DURABLE-TEST");

    @TempDir
    Path dir;

    @BeforeEach
    void resetCounters() {
        SideEffectCounters.reset();
        TestHarness.FAIL_REMAINING.clear();
    }

    private StartRequest startRequest(String id, int failures) {
        try {
            StartRequest req = new StartRequest();
            req.setWorkflowName("flaky-workflow");
            req.setInstanceId(id);
            req.setInput(new ObjectMapper().readTree("{\"orderId\":\"" + id + "\"}"));
            if (failures > 0) {
                TestHarness.FAIL_REMAINING.put("flaky-workflow:" + id, failures);
            }
            return req;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void logTrail(WorkflowInstance inst) {
        log.info("==== instance={} status={} errorType={} code={} ====",
                inst.getInstanceId(), inst.getStatus(), inst.getErrorType(), inst.getErrorCode());
        inst.getSteps().values().forEach(s -> {
            log.info("[{}] step={} status={} attempt={}",
                    inst.getInstanceId(), s.getStepName(), s.getStatus(), s.getCurrentAttempt());
            s.getAttempts().forEach(a -> log.info("[{}]   attempt#{} outcome={} type={} code={} msg={}",
                    inst.getInstanceId(), a.getAttemptNo(), a.getOutcome(),
                    a.getErrorType(), a.getErrorCode(), a.getErrorMessage()));
        });
        inst.getEvents().stream()
                .filter(e -> e.getType().contains("RETRY") || e.getType().contains("FAIL")
                        || e.getType().contains("COMPLETED"))
                .forEach(e -> log.info("[{}] retry-trail seq={} step={} attempt={} {} {}",
                        inst.getInstanceId(), e.getSeq(), e.getStepName(), e.getAttemptNo(),
                        e.getType(), e.getDetail()));
    }

    @Test
    void retryableErrorRetriesWithBackoffThenSucceedsAndEffectFiresOnce() {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);

        WorkflowInstance inst = engine.start(startRequest("wf-retry-1", 2));
        // First drive: attempt 1 fails -> RETRY_WAIT (50ms), attempt 2 is not due yet.
        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.RUNNING);
        StepState flaky = inst.getSteps().get("flaky");
        assertThat(flaky.getStatus()).isEqualTo(StepStatus.RETRY_WAIT);
        assertThat(flaky.getCurrentAttempt()).isEqualTo(1); // failed attempt; 2 scheduled
        assertThat(flaky.getErrorType()).isEqualTo(ErrorType.RETRYABLE.code());
        assertThat(flaky.getErrorCode()).isEqualTo("UPSTREAM_BUSY");

        harness.advance(49);
        inst = engine.recover("wf-retry-1");
        flaky = inst.getSteps().get("flaky");
        assertThat(flaky.getStatus()).isEqualTo(StepStatus.RETRY_WAIT); // still in backoff

        harness.advance(2); // now past the 50ms backoff: attempt 2 fails -> 100ms wait
        inst = engine.recover("wf-retry-1");
        assertThat(inst.getSteps().get("flaky").getStatus()).isEqualTo(StepStatus.RETRY_WAIT);
        assertThat(inst.getSteps().get("flaky").getCurrentAttempt()).isEqualTo(2);

        harness.advance(101);
        inst = engine.recover("wf-retry-1");
        logTrail(inst);

        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(inst.getSteps().get("flaky").getCurrentAttempt()).isEqualTo(3);
        assertThat(SideEffectCounters.executions("charge")).isEqualTo(1);
        assertThat(inst.getEvents()).anyMatch(e -> "STEP_RETRY_SCHEDULED".equals(e.getType()));
    }

    @Test
    void nonRetryableErrorFailsImmediatelyWithStableReason() {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);

        StartRequest req = new StartRequest();
        req.setWorkflowName("flaky-workflow");
        req.setInstanceId("wf-fatal-input");
        req.setInput(null); // validate rejects this

        WorkflowInstance inst = engine.start(req);
        logTrail(inst);

        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.FAILED);
        assertThat(inst.getErrorType()).isEqualTo(ErrorType.NON_RETRYABLE.code());
        assertThat(inst.getErrorCode()).isEqualTo("BAD_ORDER");
        StepState validate = inst.getSteps().get("validate");
        assertThat(validate.getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(validate.getCurrentAttempt()).isEqualTo(1); // never retried
        assertThat(inst.getSteps().get("flaky").getStatus()).isEqualTo(StepStatus.PENDING);
    }

    @Test
    void exhaustedRetriesReachTerminalFailedState() {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);

        // always fails (many more failures than 3 attempts)
        WorkflowInstance inst = engine.start(startRequest("wf-exhaust", 100));
        for (int tick = 0; tick < 10 && inst.getStatus() == InstanceStatus.RUNNING; tick++) {
            harness.advance(5_000L);
            inst = engine.recover("wf-exhaust");
        }
        logTrail(inst);

        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.FAILED);
        assertThat(inst.getErrorType()).isEqualTo(ErrorType.RETRIES_EXHAUSTED.code());
        assertThat(inst.getErrorCode()).startsWith("RETRIES_EXHAUSTED:UPSTREAM_BUSY");
        StepState flaky = inst.getSteps().get("flaky");
        assertThat(flaky.getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(flaky.getCurrentAttempt()).isEqualTo(3);
        assertThat(flaky.getAttempts()).hasSize(3);
        assertThat(SideEffectCounters.executions("charge")).isZero();

        // Driving again after terminal failure changes nothing (no rollback).
        harness.advance(60_000L);
        WorkflowInstance again = engine.recover("wf-exhaust");
        assertThat(again.getStatus()).isEqualTo(InstanceStatus.FAILED);
        assertThat(again.getSteps().get("flaky").getAttempts()).hasSize(3);
    }
}
