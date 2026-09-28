package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.ChoiceNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.TaskNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** 分支选择：依据上游输出选路，未选分支整体跳过，不卡住流程也不污染最终结果。 */
class WorkflowEngineBranchTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngineBranchTest.class);
    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;

    private WorkflowEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) engine.shutdown();
    }

    /** 输出中含 "premium" 走 premium 分支，否则走 standard 分支；其后统一有一个 after 步骤。 */
    private FlowDefinition routingWorkflow(Map<String, AtomicInteger> calls) {
        return new FlowDefinition("routing", List.of(
                new TaskNode("classify", (ctx, input) -> {
                    calls.computeIfAbsent("classify", k -> new AtomicInteger()).incrementAndGet();
                    return "tier:" + input;
                }, FAST),
                new ChoiceNode("route", output -> output.contains("premium") ? "premium" : "standard", List.of(
                        new FlowBranch("standard", List.of(
                                new TaskNode("pack", (ctx, input) -> {
                                    calls.computeIfAbsent("pack", k -> new AtomicInteger()).incrementAndGet();
                                    return "packed(" + input + ")";
                                }, FAST),
                                new TaskNode("shipStd", (ctx, input) -> {
                                    calls.computeIfAbsent("shipStd", k -> new AtomicInteger()).incrementAndGet();
                                    return "std-shipped";
                                }, FAST))),
                        new FlowBranch("premium", List.of(
                                new TaskNode("giftWrap", (ctx, input) -> {
                                    calls.computeIfAbsent("giftWrap", k -> new AtomicInteger()).incrementAndGet();
                                    return "wrapped(" + input + ")";
                                }, FAST),
                                new TaskNode("express", (ctx, input) -> {
                                    calls.computeIfAbsent("express", k -> new AtomicInteger()).incrementAndGet();
                                    return "express-shipped";
                                }, FAST))))),
                new TaskNode("after", (ctx, input) -> {
                    calls.computeIfAbsent("after", k -> new AtomicInteger()).incrementAndGet();
                    return "done(" + input + ")";
                }, FAST)));
    }

    private StepRecord findRecord(WorkflowInstance instance, String path) {
        return instance.getSteps().stream().filter(s -> path.equals(s.getPath())).findFirst().orElseThrow();
    }

    @Test
    void selectsStandardBranchAndSkipsPremiumBranch() {
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine.registerFlow(routingWorkflow(calls));

        WorkflowInstance instance = engine.start("routing", "cust-std", "standard-order");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());

        // 选中分支的两个任务恰好执行一次。
        assertEquals(1, calls.get("classify").get());
        assertEquals(1, calls.get("pack").get());
        assertEquals(1, calls.get("shipStd").get());
        assertEquals(1, calls.get("after").get());
        assertNull(calls.get("giftWrap"), "未选中分支的任务不得执行");
        assertNull(calls.get("express"));

        // 未选分支任务落为 SKIPPED，其余 COMPLETED，没有 FAILED/CANCELLED/PENDING 残留。
        assertEquals(StepStatus.SKIPPED, findRecord(terminal, "route.premium.giftWrap").getStatus());
        assertEquals(StepStatus.SKIPPED, findRecord(terminal, "route.premium.express").getStatus());
        assertEquals(StepStatus.COMPLETED, findRecord(terminal, "route.standard.pack").getStatus());
        assertEquals(StepStatus.COMPLETED, findRecord(terminal, "route.standard.shipStd").getStatus());
        assertEquals(StepStatus.COMPLETED, findRecord(terminal, "after").getStatus());

        // 数据正确传递：after 的输入是选中分支末任务输出，整体结果不受跳过任务影响。
        assertEquals("std-shipped", findRecord(terminal, "after").getInput());
        assertEquals("done(std-shipped)", findRecord(terminal, "after").getOutput());
        assertEquals("tier:standard-order", findRecord(terminal, "classify").getOutput());
    }

    @Test
    void selectsPremiumBranchAndSkipsStandardBranch() {
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine.registerFlow(routingWorkflow(calls));

        WorkflowInstance instance = engine.start("routing", "cust-prem", "premium-order");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());

        assertEquals(1, calls.get("giftWrap").get());
        assertEquals(1, calls.get("express").get());
        assertEquals(1, calls.get("after").get());
        assertNull(calls.get("pack"));
        assertNull(calls.get("shipStd"));

        assertEquals(StepStatus.SKIPPED, findRecord(terminal, "route.standard.pack").getStatus());
        assertEquals(StepStatus.SKIPPED, findRecord(terminal, "route.standard.shipStd").getStatus());
        assertEquals(StepStatus.COMPLETED, findRecord(terminal, "route.premium.express").getStatus());
        assertEquals("express-shipped", findRecord(terminal, "after").getInput());
        assertEquals("done(express-shipped)", findRecord(terminal, "after").getOutput());
    }

    @Test
    void selectorThrowingFailsInstanceImmediatelyWithoutRunningAnyBranch() {
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine.registerFlow(new FlowDefinition("badChoice", List.of(
                new TaskNode("pre", (ctx, input) -> "go", FAST),
                new ChoiceNode("decide", out -> {
                    throw new IllegalStateException("selector boom");
                }, List.of(
                        new FlowBranch("a", List.of(new TaskNode("a1", (ctx, in) -> {
                            calls.computeIfAbsent("a1", k -> new AtomicInteger()).incrementAndGet();
                            return "a";
                        }, FAST))),
                        new FlowBranch("b", List.of(new TaskNode("b1", (ctx, in) -> {
                            calls.computeIfAbsent("b1", k -> new AtomicInteger()).incrementAndGet();
                            return "b";
                        }, FAST))))))));

        WorkflowInstance instance = engine.start("badChoice", "key-bad", "in");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.FAILED, terminal.getStatus());
        assertNull(calls.get("a1"), "选择器失败后任何分支任务都不得执行");
        assertNull(calls.get("b1"));
    }

    @Test
    void selectorReturningUnknownLabelFailsInstance() {
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine.registerFlow(new FlowDefinition("unknownLabel", List.of(
                new TaskNode("pre", (ctx, input) -> "go", FAST),
                new ChoiceNode("decide", out -> "does-not-exist", List.of(
                        new FlowBranch("a", List.of(new TaskNode("a1", (ctx, in) -> "a", FAST))),
                        new FlowBranch("b", List.of(new TaskNode("b1", (ctx, in) -> "b", FAST))))))));

        WorkflowInstance instance = engine.start("unknownLabel", "key-unknown", "in");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.FAILED, terminal.getStatus());
    }

    /** 分支内部任务可重试时，沿用既有退避重试语义，已完成的上游不重跑。 */
    @Test
    void chosenBranchTaskRetriesUsingExistingSemantics() {
        AtomicInteger preCalls = new AtomicInteger();
        AtomicInteger branchCalls = new AtomicInteger();
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine.registerFlow(new FlowDefinition("retryBranch", List.of(
                new TaskNode("pre", (ctx, input) -> {
                    preCalls.incrementAndGet();
                    return "go";
                }, FAST),
                new ChoiceNode("decide", out -> "x", List.of(
                        new FlowBranch("x", List.of(new TaskNode("x1", (ctx, in) -> {
                            int n = branchCalls.incrementAndGet();
                            if (n < 2) throw new com.github.lisama569.durablefunctionruntimejava.engine.RetryableException("blip");
                            return "x-ok";
                        }, FAST))),
                        new FlowBranch("y", List.of(new TaskNode("y1", (ctx, in) -> "y", FAST))))))));

        WorkflowInstance instance = engine.start("retryBranch", "key-rb", "in");
        WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals(1, preCalls.get(), "分支任务重试不得导致上游任务重跑");
        assertEquals(2, branchCalls.get(), "分支任务应退避重试后成功");
        assertEquals(StepStatus.SKIPPED, findRecord(terminal, "decide.y.y1").getStatus());
    }
}
