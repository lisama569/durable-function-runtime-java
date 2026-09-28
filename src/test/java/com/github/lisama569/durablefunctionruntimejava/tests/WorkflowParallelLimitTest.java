package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.ControlFlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ActivityNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并行扇出上限：全局在途名额约束、单实例扇出宽度上限、多实例并发不串扰不互相拖垮。 */
class WorkflowParallelLimitTest {

    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;
    private WorkflowEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) engine.shutdown();
    }

    private ControlFlowDefinition wideWorkflow(int branches) {
        List<ParallelBranch> arms = new ArrayList<>();
        for (int i = 0; i < branches; i++) {
            String label = "b" + i;
            arms.add(new ParallelBranch(label, "do-" + label, (ctx, in) -> label, FAST));
        }
        return ControlFlowDefinition.name("wide")
                .then(new ParallelNode("work", arms, "merge",
                        (ctx, r) -> String.join(",", r.outputs()), FAST))
                .build();
    }

    @Test
    void fanOutBeyondPerInstanceLimitIsRejectedAtRegistration() {
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 3, 2);
        assertThrows(IllegalArgumentException.class,
                () -> engine.register(wideWorkflow(4)),
                "单实例扇出数超过上限必须在注册期拒绝");
    }

    @Test
    void globalInFlightPermitCapsConcurrentlyRunningBranches() throws Exception {
        // 4 个分支，全局在途名额 2：任一时刻同时在跑的分支数不得超过 2。
        int permits = 2;
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch twoStarted = new CountDownLatch(permits);

        List<ParallelBranch> arms = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String label = "b" + i;
            arms.add(new ParallelBranch(label, "do-" + label, (ctx, in) -> {
                int now = inFlight.incrementAndGet();
                maxObserved.accumulateAndGet(now, Math::max);
                twoStarted.countDown();
                release.await(10, TimeUnit.SECONDS);
                inFlight.decrementAndGet();
                return label;
            }, FAST));
        }
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, permits);
        engine.register(ControlFlowDefinition.name("cap")
                .then(new ParallelNode("work", arms, "merge",
                        (ctx, r) -> String.join(",", r.outputs()), FAST))
                .build());

        WorkflowInstance started = engine.start("cap", "key-cap", "in");
        assertTrue(twoStarted.await(5, TimeUnit.SECONDS), "应有 2 个分支先在跑");
        // 阻塞期间，在途数恰好等于名额，不能超出。
        Thread.sleep(150);
        assertEquals(permits, inFlight.get(), "名额满后其余分支必须排队，不能超额");
        release.countDown();

        WorkflowInstance terminal = engine.awaitTerminal(started.getId(), 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals(permits, maxObserved.get(), "观察到的最大在途数不得超过全局上限");
        assertEquals("b0,b1,b2,b3", terminal.getSteps().stream()
                .filter(s -> s.getNodeId().endsWith("join")).findFirst().orElseThrow().getOutput());
    }

    @Test
    void manyInstancesRunInParallelWithoutStateCrossTalkOrStarvation() {
        int instanceCount = 12;
        int branches = 3;
        // 每实例扇出 3 路，全局名额 4：必然排队，验证多实例公平推进、互不拖垮。
        Map<String, Map<String, AtomicInteger>> perInstance = new ConcurrentHashMap<>();
        List<ParallelBranch> arms = new ArrayList<>();
        for (int b = 0; b < branches; b++) {
            String label = "b" + b;
            arms.add(new ParallelBranch(label, "do-" + label, (ctx, in) -> {
                perInstance.computeIfAbsent(ctx.instanceId(), k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(label, k2 -> new AtomicInteger()).incrementAndGet();
                Thread.sleep(15);
                return ctx.instanceId() + ":" + label;
            }, FAST));
        }
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 4);
        engine.register(ControlFlowDefinition.name("multi")
                .then(new ParallelNode("work", arms, "merge", (ctx, r) -> {
                    // 每路分支结果都带实例 id 前缀，汇合结果必须只含本实例数据，验证状态不串。
                    for (String value : r.outputs()) {
                        assertTrue(value.startsWith(ctx.instanceId() + ":"),
                                "分支结果串到其他实例: " + value);
                    }
                    return String.join(";", r.outputs());
                }, FAST))
                .then(new ActivityNode("done", (ctx, in) -> in, FAST))
                .build());

        List<WorkflowInstance> startedInstances = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++) {
            startedInstances.add(engine.start("multi", "key-" + i, "payload-" + i));
        }

        for (WorkflowInstance instance : startedInstances) {
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 30_000);
            assertNotNull(terminal, "实例 " + instance.getId() + " 不应被饿死");
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            long completed = terminal.getSteps().stream()
                    .filter(s -> s.getStatus() == StepStatus.COMPLETED).count();
            assertEquals(branches + 2, completed, "3 分支 + join + done 都应完成");
            String joinOutput = terminal.getSteps().stream()
                    .filter(s -> s.getNodeId().endsWith("join")).findFirst().orElseThrow().getOutput();
            // 每个实例的 join 输出恰好包含自己实例的 3 路结果，不含任何其他实例数据。
            for (int b = 0; b < branches; b++) {
                String token = instance.getId() + ":b" + b;
                assertTrue(joinOutput.contains(token), "join 缺少本实例分支结果: " + token);
            }
            assertEquals(3, joinOutput.split(";").length, "join 输出只能含本实例的 3 路结果");
        }

        // 每个实例的每路分支恰好执行一次，跨实例计数不串。
        for (WorkflowInstance instance : startedInstances) {
            var counts = perInstance.get(instance.getId());
            assertNotNull(counts, "实例 " + instance.getId() + " 应有分支执行记录");
            for (int b = 0; b < branches; b++) {
                assertEquals(1, counts.get("b" + b).get(),
                        "实例 " + instance.getId() + " 分支 b" + b + " 必须恰好执行一次");
            }
        }
    }
}
