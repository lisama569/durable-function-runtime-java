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
import com.github.lisama569.durablefunctionruntimejava.engine.function.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.function.WorkflowDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.json.Json;
import com.github.lisama569.durablefunctionruntimejava.engine.model.AttemptRecord;
import com.github.lisama569.durablefunctionruntimejava.engine.model.InstanceStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StartRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepState;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowEvent;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.SideEffectCounters;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.TestHarness;

/**
 * Simulates an engine/JVM restart: a second engine over the same storage
 * recovers an interrupted attempt, never re-executes committed effects and
 * never rolls completed steps back.
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger("DURABLE-TEST");

    @TempDir
    Path dir;

    @BeforeEach
    void reset() {
        SideEffectCounters.reset();
        TestHarness.FAIL_REMAINING.clear();
    }

    private static com.fasterxml.jackson.databind.JsonNode parse(String json) {
        try {
            return new ObjectMapper().readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void trail(String tag, WorkflowInstance inst) {
        log.info("---- {} instance={} status={} stepIndex={} ----",
                tag, inst.getInstanceId(), inst.getStatus(), inst.getCurrentStepIndex());
        for (StepState s : inst.getSteps().values()) {
            log.info("[{}] step={} status={} attempt={} attemptsRecorded={} lastOutcome={}",
                    inst.getInstanceId(), s.getStepName(), s.getStatus(), s.getCurrentAttempt(),
                    s.getAttempts().size(),
                    s.getAttempts().isEmpty() ? "-"
                            : s.getAttempts().get(s.getAttempts().size() - 1).getOutcome());
        }
        inst.getEvents().forEach(e -> log.info("[{}] seq={} {} step={} attempt={} {}",
                inst.getInstanceId(), e.getSeq(), e.getType(),
                e.getStepName() == null ? "-" : e.getStepName(),
                e.getAttemptNo() == null ? "-" : e.getAttemptNo(),
                e.getDetail() == null ? "" : e.getDetail()));
    }

    @Test
    void completedWorkflowsSurviveRestartAndAreNotReplayed() {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine1 = harness.engine(dir);

        StartRequest req = new StartRequest();
        req.setWorkflowName("flaky-workflow");
        req.setInstanceId("wf-restart-done");
        req.setInput(parse("{\"orderId\":\"o-1\"}"));
        WorkflowInstance done = engine1.start(req);
        assertThat(done.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(SideEffectCounters.executions("charge")).isEqualTo(1);

        // "Restart": fresh engine/registry/store over the same files.
        SideEffectCounters.reset();
        DurableWorkflowEngine engine2 = harness.engine(dir);
        WorkflowInstance reloaded = engine2.get("wf-restart-done");
        trail("after restart", reloaded);

        assertThat(reloaded.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        WorkflowInstance recovered = engine2.recover("wf-restart-done");
        assertThat(recovered.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(SideEffectCounters.executions("charge")).isZero();
    }

    /**
     * Builds the exact on-disk artefact a hard JVM kill leaves behind when a
     * synchronous step was executing: aggregate committed at the attempt
     * boundary, first step status=RUNNING with an open attempt record.
     */
    private WorkflowInstance crashedRunningInstance(String instanceId) {
        WorkflowInstance inst = new WorkflowInstance();
        inst.setInstanceId(instanceId);
        inst.setWorkflowName("blocking-workflow");
        inst.setStatus(InstanceStatus.RUNNING);
        inst.setCurrentStepIndex(0);
        inst.setCreatedAtMs(1_000_000L);
        inst.setUpdatedAtMs(1_000_000L);
        inst.setInput(parse("{}"));

        StepState blocking = new StepState("blocking");
        blocking.setStatus(StepStatus.RUNNING);
        blocking.setCurrentAttempt(1);
        blocking.getAttempts().add(AttemptRecord.start(1, 1_000_000L));
        inst.getSteps().put("blocking", blocking);
        inst.getSteps().put("after", new StepState("after"));
        inst.getEvents().add(new WorkflowEvent(1, 1_000_000L, "STARTED", null, null,
                "workflow=blocking-workflow"));
        inst.getEvents().add(new WorkflowEvent(2, 1_000_000L, "STEP_ATTEMPT_START",
                "blocking", 1, "sync attempt 1/2"));
        return inst;
    }

    @Test
    void inFlightStepInterruptedByRestartIsRecoveredAndRetried() {
        TestHarness harness = new TestHarness();

        // Original deployment had a blocking step; after the restart the code
        // is re-registered with a replacement implementation.
        harness.registry().register(WorkflowDefinition.builder("blocking-workflow")
                .addStep(StepDefinition.sync("blocking",
                        new RetryPolicy(2, 0L, 1.0, 10L, 0L),
                        ctx -> {
                            throw new IllegalStateException("must never run on recovery");
                        }))
                .addStep(StepDefinition.sync("after", RetryPolicy.NO_RETRY, ctx -> ctx.input()))
                .build());
        harness.engine(dir); // ensure storage directories exist
        writeCrashFile(dir, crashedRunningInstance("wf-crash-1"));

        // restart with recovered workflow definition
        DurableWorkflowEngine engine2 = harness.engine(dir);
        harness.registry().register(WorkflowDefinition.builder("blocking-workflow")
                .addStep(StepDefinition.sync("blocking",
                        new RetryPolicy(2, 0L, 1.0, 10L, 0L),
                        ctx -> Json.newObject().put("done", true).put("recovered", true)))
                .addStep(StepDefinition.sync("after", RetryPolicy.NO_RETRY, ctx -> ctx.input()))
                .build());

        WorkflowInstance recovered = engine2.recover("wf-crash-1");
        trail("recovered", recovered);

        assertThat(recovered.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        StepState blocking = recovered.getSteps().get("blocking");
        assertThat(blocking.getOutput().path("recovered").asBoolean()).isTrue();
        assertThat(blocking.getAttempts()).anyMatch(a -> "INTERRUPTED".equals(a.getOutcome()));
        assertThat(recovered.getEvents())
                .anyMatch(e -> "RECOVERED_AFTER_RESTART".equals(e.getType()));

        // repeated recovery does not append phantom attempts
        WorkflowInstance again = engine2.recover("wf-crash-1");
        assertThat(again.getSteps().get("blocking").getAttempts())
                .hasSameSizeAs(blocking.getAttempts());
    }

    private void writeCrashFile(Path storageDir, WorkflowInstance inst) {
        try {
            Path f = storageDir.resolve("instances").resolve(inst.getInstanceId() + ".json");
            java.nio.file.Files.createDirectories(f.getParent());
            java.nio.file.Files.write(f, new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(inst));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void interruptedBackoffStateSurvivesRestartAndHonoursDeadline() {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine1 = harness.engine(dir);
        StartRequest req = new StartRequest();
        req.setWorkflowName("flaky-workflow");
        req.setInstanceId("wf-restart-backoff");
        req.setInput(parse("{\"orderId\":\"o-9\"}"));
        TestHarness.FAIL_REMAINING.put("flaky-workflow:wf-restart-backoff", 1);
        WorkflowInstance inst = engine1.start(req);
        assertThat(inst.getSteps().get("flaky").getStatus()).isEqualTo(StepStatus.RETRY_WAIT);
        long notBefore = inst.getSteps().get("flaky").getNotBeforeMs();

        DurableWorkflowEngine engine2 = harness.engine(dir);
        WorkflowInstance waiting = engine2.recover("wf-restart-backoff");
        assertThat(waiting.getSteps().get("flaky").getStatus()).isEqualTo(StepStatus.RETRY_WAIT);
        assertThat(waiting.getSteps().get("flaky").getNotBeforeMs()).isEqualTo(notBefore);

        harness.advance(100);
        WorkflowInstance done = engine2.recover("wf-restart-backoff");
        trail("after backoff restart", done);
        assertThat(done.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(SideEffectCounters.executions("charge")).isEqualTo(1);
    }
}
