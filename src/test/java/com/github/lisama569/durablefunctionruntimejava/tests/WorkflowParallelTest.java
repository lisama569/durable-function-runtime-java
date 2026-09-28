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
import com.github.lisama569.durablefunctionruntimejava.workflow.node.BranchResults;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并行扇出/汇合：乱序完成结果确定、汇合恰好一次、各分支恰好执行一次。 */
class WorkflowParallelTest {

    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;
    private WorkflowEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) engine.shutdown();
    }

    private StepRecord step(WorkflowInstance instance, String nodeIdSuffix) {
        return instance.getSteps().stream()
                .filter(s -> s.getNodeId().endsWith(nodeIdSuffix))
                .findFirst().orElseThrow(() -> new AssertionError("missing step " + nodeIdSuffix));
    }

    /** 构造三路并行：分支延迟由 delays 决定（毫秒），便于强制乱序完成。 */
    private WorkflowEngine newFanOutWorkflow(Map<String, AtomicInteger> calls,
                                             AtomicInteger joinCalls,
                                             long delayX, long delayY, long delayZ) {
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        engine.register(ControlFlowDefinition.name("fanout")
                .then(new ActivityNode("prepare", (ctx, in) -> "base", FAST))
                .then(new ParallelNode("work", List.of(
                        new ParallelBranch("x", "doX", (ctx, in) -> {
                            calls.computeIfAbsent("x", k -> new AtomicInteger()).incrementAndGet();
                            Thread.sleep(delayX);
                            return "X";
                        }, FAST),
                        new ParallelBranch("y", "doY", (ctx, in) -> {
                            calls.computeIfAbsent("y", k -> new AtomicInteger()).incrementAndGet();
                            Thread.sleep(delayY);
                            return "Y";
                        }, FAST),
                        new ParallelBranch("z", "doZ", (ctx, in) -> {
                            calls.computeIfAbsent("z", k -> new AtomicInteger()).incrementAndGet();
                            Thread.sleep(delayZ);
                            return "Z";
                        }, FAST)),
                        "merge",
                        (ctx, results) -> {
                            joinCalls.incrementAndGet();
                            // 用定义顺序重放，验证与完成先后无关。
                            assertEquals(List.of("x", "y", "z"), results.labels(),
                                    "汇合标签必须按定义顺序");
                            return String.join("|", results.outputs());
                        }, FAST))
                .then(new ActivityNode("finish", (ctx, in) -> "done:" + in, FAST))
                .build());
        return engine;
    }

    @Test
    void joinRunsOnceWithDefinitionOrderRegardlessOfCompletionOrder() {
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        AtomicInteger joinCalls = new AtomicInteger();
        // x 最慢、y 最快、z 中间：完成顺序 y,z,x。
        newFanOutWorkflow(calls, joinCalls, 160, 10, 70);

        WorkflowInstance started = engine.start("fanout", "key-1", "in");
        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);

        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals(1, joinCalls.get(), "汇合必须恰好执行一次");
        assertEquals(1, calls.get("x").get());
        assertEquals(1, calls.get("y").get());
        assertEquals(1, calls.get("z").get(), "每个分支恰好执行一次");
        // 汇合输出按定义顺序 x,y,z，即使实际完成顺序是 y,z,x。
        assertEquals("X|Y|Z", step(terminal, "work/join").getOutput());
        assertEquals("done:X|Y|Z", step(terminal, "finish").getOutput());
        Map<String, String> branchOut = terminal.getBranchOutputs().get("work");
        assertEquals(List.of("x", "y", "z"), List.copyOf(branchOut.keySet()),
                "持久化的分支输出按定义顺序排列");
    }

    @Test
    void differentCompletionOrdersProduceIdenticalJoinResult() {
        long[][] delayPlans = {
                {120, 10, 60},   // y,z,x
                {10, 120, 60},   // x,z,y
                {60, 120, 10},   // z,x,y
                {10, 10, 10}     // 几乎同时
        };
        for (int planIndex = 0; planIndex < delayPlans.length; planIndex++) {
            final int plan = planIndex;
            final long[] delays = delayPlans[plan];
            Path subDir = dataDir.resolve("plan-" + plan);
            Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
            AtomicInteger joinCalls = new AtomicInteger();
            WorkflowEngine e = new WorkflowEngine(new FileWorkflowStore(subDir), 16, 8);
            try {
                e.register(ControlFlowDefinition.name("fanout")
                        .then(new ActivityNode("prepare", (ctx, in) -> "base", FAST))
                        .then(new ParallelNode("work", List.of(
                                new ParallelBranch("x", "doX", (ctx, in) -> {
                                    calls.computeIfAbsent("x", k -> new AtomicInteger()).incrementAndGet();
                                    Thread.sleep(delays[0]);
                                    return "X";
                                }, FAST),
                                new ParallelBranch("y", "doY", (ctx, in) -> {
                                    calls.computeIfAbsent("y", k -> new AtomicInteger()).incrementAndGet();
                                    Thread.sleep(delays[1]);
                                    return "Y";
                                }, FAST),
                                new ParallelBranch("z", "doZ", (ctx, in) -> {
                                    calls.computeIfAbsent("z", k -> new AtomicInteger()).incrementAndGet();
                                    Thread.sleep(delays[2]);
                                    return "Z";
                                }, FAST)),
                                "merge",
                                (ctx, results) -> String.join("|", results.outputs()), FAST))
                        .then(new ActivityNode("finish", (ctx, in) -> "done:" + in, FAST))
                        .build());
                WorkflowInstance started = e.start("fanout", "k", "in");
                WorkflowInstance terminal = e.awaitTerminal(started.getId(), 10_000);
                assertNotNull(terminal, "plan " + plan + " 应进入终态");
                assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
                assertEquals("done:X|Y|Z", terminal.getSteps().stream()
                        .filter(s -> "finish".equals(s.getName())).findFirst().orElseThrow().getOutput(),
                        "plan " + plan + " 的汇合结果必须一致");
            } finally {
                e.shutdown();
            }
        }
    }

    @Test
    void everyBranchReceivesSameFanOutInput() {
        Map<String, String> seenInputs = new ConcurrentHashMap<>();
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        engine.register(ControlFlowDefinition.name("fanout2")
                .then(new ActivityNode("prepare", (ctx, in) -> "payload-42", FAST))
                .then(new ParallelNode("work", List.of(
                        new ParallelBranch("a", "doA", (ctx, in) -> {
                            seenInputs.put("a", in);
                            return "A";
                        }, FAST),
                        new ParallelBranch("b", "doB", (ctx, in) -> {
                            seenInputs.put("b", in);
                            return "B";
                        }, FAST)),
                        "merge",
                        (ctx, results) -> results.get("a") + "+" + results.get("b"), FAST))
                .build());
        WorkflowInstance started = engine.start("fanout2", "key-in", "in");
        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals("payload-42", seenInputs.get("a"));
        assertEquals("payload-42", seenInputs.get("b"), "每路分支拿到相同的扇出输入");
        assertEquals("A+B", step(terminal, "work/join").getOutput());
    }

    @Test
    void allActivitiesHaveDeterministicStatusOnCompletion() {
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        newFanOutWorkflow(calls, new AtomicInteger(), 5, 5, 5);
        WorkflowInstance started = engine.start("fanout", "key-status", "in");
        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);
        assertNotNull(terminal);
        long completed = terminal.getSteps().stream()
                .filter(s -> s.getStatus() == StepStatus.COMPLETED).count();
        // prepare + 3 分支 + join + finish = 6 个活动全部完成，无 PENDING/RUNNING/FAILED 残留。
        assertEquals(6, completed);
        assertTrue(terminal.getSteps().stream().allMatch(s -> s.getStatus() == StepStatus.COMPLETED));
        assertTrue(terminal.getOpenParallels().isEmpty(), "汇合后并行组应关闭");
    }
}
