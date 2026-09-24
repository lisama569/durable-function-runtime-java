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
import com.github.lisama569.durablefunctionruntimejava.engine.function.AsyncTask;
import com.github.lisama569.durablefunctionruntimejava.engine.model.CompletionRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.InstanceStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StartRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepState;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.ManualAsync;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.TestHarness;

/**
 * Async semantics: WAITING on dispatch, resume on token-bound callback,
 * duplicate/out-of-order callbacks are ignored, failed callbacks retry with
 * dispatch of a new token, timeout drives the attempt to a terminal state.
 */
class AsyncCallbackTest {

    private static final Logger log = LoggerFactory.getLogger("DURABLE-TEST");

    @TempDir
    Path dir;

    @BeforeEach
    void reset() {
        TestHarness.FAIL_REMAINING.clear();
    }

    private StartRequest req(String id) {
        StartRequest r = new StartRequest();
        r.setWorkflowName("async-workflow");
        r.setInstanceId(id);
        r.setInput(parse("{\"orderId\":\"" + id + "\"}"));
        return r;
    }

    private ManualAsync dispatcher(TestHarness harness) {
        return harness.asyncDispatcher();
    }

    private CompletionRequest callback(String token, boolean ok, String result) throws Exception {
        CompletionRequest c = new CompletionRequest();
        c.setToken(token);
        c.setSuccess(ok);
        if (ok) {
            c.setResult(new ObjectMapper().readTree(result));
            c.setDedupeKey("approval:" + token);
        } else {
            c.setErrorCode("APPROVAL_REJECTED");
            c.setErrorMessage("approver said no");
        }
        return c;
    }

    private void trail(String tag, WorkflowInstance inst) {
        log.info("---- {} {} status={} ----", tag, inst.getInstanceId(), inst.getStatus());
        for (StepState s : inst.getSteps().values()) {
            log.info("[{}] step={} status={} attempt={} token={}", inst.getInstanceId(),
                    s.getStepName(), s.getStatus(), s.getCurrentAttempt(), s.getActiveToken());
            s.getAttempts().forEach(a -> log.info("[{}]   #{} {} {} {}", inst.getInstanceId(),
                    a.getAttemptNo(), a.getOutcome(), a.getErrorType(), a.getErrorCode()));
        }
        inst.getEvents().forEach(e -> log.info("[{}] {} step={} attempt={} {}",
                inst.getInstanceId(), e.getType(), e.getStepName(), e.getAttemptNo(),
                e.getDetail() == null ? "" : e.getDetail()));
    }

    @Test
    void waitsForCallbackThenCompletes() throws Exception {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);
        ManualAsync dispatcher = dispatcher(harness);

        WorkflowInstance inst = engine.start(req("wf-async-ok"));
        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.RUNNING);
        StepState waiting = inst.getSteps().get("awaitApproval");
        assertThat(waiting.getStatus()).isEqualTo(StepStatus.WAITING);
        assertThat(dispatcher.dispatchCount).isEqualTo(1);

        AsyncTask task = dispatcher.lastTask();
        inst = engine.complete(callback(task.token(), true, "{\"approved\":true}"));
        trail("after callback", inst);

        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(inst.getSteps().get("awaitApproval").getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(inst.getOutput().path("approved").asBoolean()).isTrue();
    }

    @Test
    void duplicateAndOutOfOrderCallbacksAreIgnored() throws Exception {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);
        ManualAsync dispatcher = dispatcher(harness);

        engine.start(req("wf-async-dup"));
        String token1 = dispatcher.lastTask().token();

        // 1) success completes attempt 1
        WorkflowInstance inst = engine.complete(callback(token1, true, "{\"approved\":true}"));
        assertThat(inst.getSteps().get("awaitApproval").getStatus()).isEqualTo(StepStatus.COMPLETED);

        // 2) exact duplicate of the same token: no effect (token already consumed)
        WorkflowInstance dup = engine.complete(callback(token1, true, "{\"approved\":false}"));
        // 3) out-of-order "failure" for the already-completed attempt: ignored
        WorkflowInstance late = engine.complete(callback(token1, false, null));
        // 4) completely unknown token: ignored (returns null instance)
        WorkflowInstance unknown = engine.complete(callback("deadbeefdeadbeefdeadbeef", true, "{}"));
        if (dup == null) {
            dup = engine.get("wf-async-dup");
        }
        if (late == null) {
            late = engine.get("wf-async-dup");
        }

        trail("after duplicate/late callbacks", dup);
        assertThat(dup).isNotNull();
        assertThat(dup.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(dup.getOutput().path("approved").asBoolean()).isTrue();
        assertThat(late.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(unknown).isNull();
        assertThat(dup.getEvents()).anyMatch(e -> e.getType().contains("CALLBACK_IGNORED")
                || e.getType().contains("DEDUP"));
    }

    @Test
    void failedCallbackRedrivesWithNewTokenAndExhaustsToTerminal() throws Exception {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);
        ManualAsync dispatcher = dispatcher(harness);

        engine.start(req("wf-async-fail"));
        String token1 = dispatcher.lastTask().token();

        WorkflowInstance inst = engine.complete(callback(token1, false, null));
        assertThat(inst.getSteps().get("awaitApproval").getStatus()).isEqualTo(StepStatus.RETRY_WAIT);
        assertThat(inst.getSteps().get("awaitApproval").getAttempts()).hasSize(1);

        harness.advance(10); // 5ms backoff
        inst = engine.recover("wf-async-fail");
        assertThat(dispatcher.dispatchCount).isEqualTo(2);
        String token2 = dispatcher.lastTask().token();
        assertThat(token2).isNotEqualTo(token1);
        assertThat(inst.getSteps().get("awaitApproval").getStatus()).isEqualTo(StepStatus.WAITING);

        inst = engine.complete(callback(token2, false, null));
        harness.advance(6);
        inst = engine.recover("wf-async-fail"); // attempt 3 dispatched
        assertThat(inst.getSteps().get("awaitApproval").getStatus()).isEqualTo(StepStatus.WAITING);
        harness.advance(51);                    // attempt 3 times out -> retries exhausted
        inst = engine.recover("wf-async-fail");
        trail("after exhausting async retries", inst);

        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.FAILED);
        assertThat(inst.getErrorType()).isEqualTo("RETRIES_EXHAUSTED");
        assertThat(inst.getSteps().get("awaitApproval").getStatus()).isEqualTo(StepStatus.FAILED);
        // the stale token1 callback must still be rejected (idempotent no-op)
        WorkflowInstance late = engine.complete(callback(token1, true, "{\"approved\":true}"));
        assertThat(late).isNotNull(); // routed to the instance via recorded token
        assertThat(late.getStatus()).isEqualTo(InstanceStatus.FAILED); // state untouched
        assertThat(late.getSteps().get("awaitApproval").getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(late.getEvents()).anyMatch(e -> "CALLBACK_IGNORED".equals(e.getType()));
    }

    @Test
    void asyncAttemptTimesOutAndRetriesThenSucceeds() throws Exception {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);
        ManualAsync dispatcher = dispatcher(harness);

        engine.start(req("wf-async-timeout"));
        String token1 = dispatcher.lastTask().token();

        // deadline 50ms passes without a callback -> TIMEOUT -> retry (new dispatch)
        harness.advance(51);
        WorkflowInstance inst = engine.recover("wf-async-timeout"); // timeout -> RETRY_WAIT(5ms)
        harness.advance(6);
        inst = engine.recover("wf-async-timeout");                  // retry dispatched
        trail("after timeout", inst);
        assertThat(dispatcher.dispatchCount).isEqualTo(2);
        StepState step = inst.getSteps().get("awaitApproval");
        assertThat(step.getStatus()).isEqualTo(StepStatus.WAITING);
        assertThat(step.getAttempts().get(0).getOutcome()).isEqualTo("TIMED_OUT");

        // late callback for the timed-out attempt is rejected
        WorkflowInstance rejected = engine.complete(callback(token1, true, "{\"approved\":true}"));
        assertThat(rejected.getSteps().get("awaitApproval").getStatus())
                .isEqualTo(StepStatus.WAITING);

        String token2 = dispatcher.lastTask().token();
        inst = engine.complete(callback(token2, true, "{\"approved\":true}"));
        trail("after retry success", inst);
        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(inst.getSteps().get("awaitApproval").getAttempts()).hasSize(2);
    }

    private static com.fasterxml.jackson.databind.JsonNode parse(String json) {
        try {
            return new ObjectMapper().readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
