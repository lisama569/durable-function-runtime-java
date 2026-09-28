package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.ControlFlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ActivityNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ChoiceBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ChoiceNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** 条件分支：按上游输出选路，未选分支跳过、不阻塞、不影响终态与结果。 */
class WorkflowBranchTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowBranchTest.class);
    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;
    private WorkflowEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) engine.shutdown();
    }

    private WorkflowEngine engine() {
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        return engine;
    }

    private ControlFlowDefinition routingWorkflow(AtomicInteger premiumCalls,
                                                  AtomicInteger standardCalls,
                                                  AtomicInteger fallbackCalls) {
        return ControlFlowDefinition.name("routing")
                .then(new ActivityNode("classify", (ctx, in) -> in, FAST))
                .then(new ChoiceNode("route", List.of(
                        new ChoiceBranch("premium", out -> "premium".equals(out), List.of(
                                new ActivityNode("handlePremium", (ctx, in) -> {
                                    premiumCalls.incrementAndGet();
                                    return "P:" + in;
                                }, FAST))),
                        new ChoiceBranch("standard", out -> "standard".equals(out), List.of(
                                new ActivityNode("handleStandard", (ctx, in) -> {
                                    standardCalls.incrementAndGet();
                                    return "S:" + in;
                                }, FAST))),
                        new ChoiceBranch("fallback", null, List.of(
                                new ActivityNode("handleFallback", (ctx, in) -> {
                                    fallbackCalls.incrementAndGet();
                                    return "F:" + in;
                                }, FAST))))))
                .then(new ActivityNode("notify", (ctx, in) -> "notified:" + in, FAST))
                .build();
    }

    private StepRecord step(WorkflowInstance instance, String nodeIdSuffix) {
        return instance.getSteps().stream()
                .filter(s -> s.getNodeId().endsWith(nodeIdSuffix))
                .findFirst().orElseThrow(() -> new AssertionError("missing step " + nodeIdSuffix));
    }

    @Test
    void selectedBranchRunsOthersAreSkippedAndFlowCompletes() {
        AtomicInteger premium = new AtomicInteger();
        AtomicInteger standard = new AtomicInteger();
        AtomicInteger fallback = new AtomicInteger();
        engine().register(routingWorkflow(premium, standard, fallback));

        WorkflowInstance started = engine.start("routing", "key-premium", "premium");
        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals("premium", terminal.getChoices().get("route"));
        assertEquals(1, premium.get());
        assertEquals(0, standard.get(), "未选分支不得执行");
        assertEquals(0, fallback.get(), "未选分支不得执行");
        assertEquals(StepStatus.COMPLETED, step(terminal, "handlePremium").getStatus());
        assertEquals(StepStatus.SKIPPED, step(terminal, "handleStandard").getStatus());
        assertEquals(StepStatus.SKIPPED, step(terminal, "handleFallback").getStatus());
        // 汇合后的收尾步骤照常执行，整体结果不受跳过分支影响。
        assertEquals("notified:P:premium", step(terminal, "notify").getOutput());
    }

    @Test
    void fallbackBranchIsChosenWhenNoConditionMatches() {
        AtomicInteger premium = new AtomicInteger();
        AtomicInteger standard = new AtomicInteger();
        AtomicInteger fallback = new AtomicInteger();
        engine().register(routingWorkflow(premium, standard, fallback));

        WorkflowInstance started = engine.start("routing", "key-unknown", "weird");
        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals("fallback", terminal.getChoices().get("route"));
        assertEquals(1, fallback.get());
        assertEquals(0, premium.get());
        assertEquals(0, standard.get());
        assertEquals(StepStatus.SKIPPED, step(terminal, "handlePremium").getStatus());
        assertEquals("notified:F:weird", step(terminal, "notify").getOutput());
    }

    @Test
    void skippedBranchesDoNotBlockAndCountsAreDeterministicAcrossRuns() {
        // 多次运行同一类输入，跳过的分支计数必须恒为 0，命中分支恰好 1 次。
        for (int run = 0; run < 5; run++) {
            AtomicInteger standard = new AtomicInteger();
            WorkflowEngine e = new WorkflowEngine(new FileWorkflowStore(dataDir));
            try {
                AtomicInteger premium = new AtomicInteger();
                AtomicInteger fallback = new AtomicInteger();
                e.register(routingWorkflow(premium, standard, fallback));
                WorkflowInstance started = e.start("routing", "key-std-" + run, "standard");
                WorkflowInstance terminal = e.awaitTerminal(started.getId(), 10_000);
                assertNotNull(terminal);
                assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
                assertEquals(1, standard.get());
                assertEquals(0, premium.get());
                assertEquals(0, fallback.get());
                assertEquals("notified:S:standard", step(terminal, "notify").getOutput());
                // 每个实例恰好有 3 个活动（classify/selected handler/notify）完成，其余跳过。
                long completed = terminal.getSteps().stream()
                        .filter(s -> s.getStatus() == StepStatus.COMPLETED).count();
                long skipped = terminal.getSteps().stream()
                        .filter(s -> s.getStatus() == StepStatus.SKIPPED).count();
                assertEquals(3, completed);
                assertEquals(2, skipped);
            } finally {
                e.shutdown();
            }
        }
    }
}
