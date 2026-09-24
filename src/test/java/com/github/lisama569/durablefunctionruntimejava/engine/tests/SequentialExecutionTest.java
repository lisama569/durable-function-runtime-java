package com.github.lisama569.durablefunctionruntimejava.engine.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.lisama569.durablefunctionruntimejava.engine.DurableWorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.engine.model.InstanceStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StartRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.TestHarness;

/** Happy path: steps advance in order, inputs/outputs persist, trail is logged. */
class SequentialExecutionTest {

    private static final Logger log = LoggerFactory.getLogger("DURABLE-TEST");

    @TempDir
    Path dir;

    private void printTrail(WorkflowInstance inst) {
        log.info("==== trail instance={} status={} ====", inst.getInstanceId(), inst.getStatus());
        inst.getSteps().values().forEach(s ->
                log.info("[{}] step={} status={} attempts={} errorType={} code={}",
                        inst.getInstanceId(), s.getStepName(), s.getStatus(),
                        s.getAttempts().size(), s.getErrorType(), s.getErrorCode()));
        inst.getEvents().forEach(e ->
                log.info("[{}] seq={} step={} attempt={} type={} {}",
                        inst.getInstanceId(), e.getSeq(),
                        e.getStepName() == null ? "-" : e.getStepName(),
                        e.getAttemptNo() == null ? "-" : e.getAttemptNo(),
                        e.getType(), e.getDetail() == null ? "" : e.getDetail()));
    }

    @Test
    void runsStepsInOrderAndPersistsOutputs() throws Exception {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);

        StartRequest req = new StartRequest();
        req.setWorkflowName("flaky-workflow");
        req.setInstanceId("wf-seq-1");
        JsonNode input = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree("{\"orderId\":\"order-100\"}");
        req.setInput(input);

        WorkflowInstance inst = engine.start(req);
        printTrail(inst);

        assertThat(inst.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(inst.getCurrentStepIndex()).isEqualTo(3);
        assertThat(inst.getSteps().get("validate").getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(inst.getSteps().get("flaky").getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(inst.getSteps().get("echo").getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(inst.getSteps().get("flaky").getCurrentAttempt()).isEqualTo(1);
        assertThat(inst.getOutput().path("charge").path("orderId").asText())
                .isEqualTo("order-100");

        // persisted file is directly readable
        Path stateFile = dir.resolve("instances").resolve("wf-seq-1.json");
        assertThat(Files.exists(stateFile)).isTrue();
        assertThat(Files.size(stateFile)).isGreaterThan(0L);

        // journal printed for humans too
        Path journal = dir.resolve("test-seq-trail.txt");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(journal))) {
            inst.getEvents().forEach(e -> w.println(e.getSeq() + " " + e.getType()
                    + " " + e.getStepName()));
        }
        assertThat(Files.readString(journal)).contains("STARTED").contains("WORKFLOW_COMPLETED");
    }
}
