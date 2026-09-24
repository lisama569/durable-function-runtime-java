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
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.SideEffectCounters;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.TestHarness;

/**
 * Duplicate submission must not re-execute steps, and terminate must settle
 * the instance in a terminal state that subsequent drives/callbacks cannot
 * roll back.
 */
class DuplicateSubmissionAndTerminationTest {

    private static final Logger log = LoggerFactory.getLogger("DURABLE-TEST");

    @TempDir
    Path dir;

    @BeforeEach
    void reset() {
        SideEffectCounters.reset();
        TestHarness.FAIL_REMAINING.clear();
    }

    private StartRequest startRequest(String id) {
        try {
            StartRequest req = new StartRequest();
            req.setWorkflowName("flaky-workflow");
            req.setInstanceId(id);
            req.setInput(new ObjectMapper().readTree("{\"orderId\":\"" + id + "\"}"));
            return req;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void duplicateStartAfterCompletionDoesNotReplayAnything() {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);

        WorkflowInstance first = engine.start(startRequest("wf-dup-1"));
        assertThat(first.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(SideEffectCounters.executions("charge")).isEqualTo(1);
        int eventCount = first.getEvents().size();

        SideEffectCounters.reset();
        WorkflowInstance second = engine.start(startRequest("wf-dup-1"));
        WorkflowInstance third = engine.start(startRequest("wf-dup-1"));

        log.info("[{}] events after resubmit:", third.getInstanceId());
        third.getEvents().forEach(e -> log.info("  seq={} {} {}", e.getSeq(), e.getType(),
                e.getDetail() == null ? "" : e.getDetail()));

        assertThat(second.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(third.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(third.getSteps().get("validate").getCurrentAttempt()).isEqualTo(1);
        assertThat(third.getSteps().get("flaky").getCurrentAttempt()).isEqualTo(1);
        assertThat(SideEffectCounters.executions("charge")).isZero();
        // each duplicate submission is itself auditable but changes nothing
        assertThat(third.getEvents().size()).isEqualTo(eventCount + 2);
        assertThat(third.getEvents())
                .filteredOn(e -> "START_DEDUPED".equals(e.getType())).hasSize(2);

        // same state after "restart" + duplicate start
        DurableWorkflowEngine engine2 = harness.engine(dir);
        SideEffectCounters.reset();
        WorkflowInstance fourth = engine2.start(startRequest("wf-dup-1"));
        assertThat(fourth.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(SideEffectCounters.executions("charge")).isZero();
    }

    @Test
    void terminateReachesTerminalStateAndCannotBeRolledBack() {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);

        // Flaky step parked in RETRY_WAIT gives us a stable RUNNING instance.
        TestHarness.FAIL_REMAINING.put("flaky-workflow:wf-term-1", 10);
        WorkflowInstance inst = engine.start(startRequest("wf-term-1"));
        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.RUNNING);

        WorkflowInstance terminated = engine.terminate("wf-term-1", "customer cancelled");
        log.info("terminated status={} type={} code={}", terminated.getStatus(),
                terminated.getErrorType(), terminated.getErrorCode());
        terminated.getEvents().forEach(e -> log.info("  {} {}", e.getType(), e.getDetail()));

        assertThat(terminated.getStatus()).isEqualTo(InstanceStatus.TERMINATED);
        assertThat(terminated.getErrorType()).isEqualTo(ErrorType.TERMINATED.code());

        // further drives, terminates and starts cannot roll state back
        harness.advance(60_000L);
        WorkflowInstance driven = engine.recover("wf-term-1");
        WorkflowInstance terminatedAgain = engine.terminate("wf-term-1", "again");
        WorkflowInstance restarter = engine.start(startRequest("wf-term-1"));

        for (WorkflowInstance i : java.util.List.of(driven, terminatedAgain, restarter)) {
            assertThat(i.getStatus()).isEqualTo(InstanceStatus.TERMINATED);
            assertThat(i.getSteps().get("flaky").getCurrentAttempt())
                    .isEqualTo(terminated.getSteps().get("flaky").getCurrentAttempt());
        }
        assertThat(terminatedAgain.getEvents())
                .anyMatch(e -> "TERMINATE_IGNORED".equals(e.getType()));
    }
}
