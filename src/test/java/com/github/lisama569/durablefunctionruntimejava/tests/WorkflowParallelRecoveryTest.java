package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.ControlFlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDef;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ActivityNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelNode;
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

/** 并行执行中途崩溃恢复：已完成分支不重跑，仅补未完成分支，汇合恰好一次，结果确定。 */
class WorkflowParallelRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowParallelRecoveryTest.class);
    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;

    private WorkflowDef fanoutWorkflow(AtomicInteger x, AtomicInteger y, AtomicInteger z,
                                       AtomicInteger join, CountDownLatch zEntered,
                                       boolean crashBeforeRestart) {
        return ControlFlowDefinition.name("pfan")
                .then(new ActivityNode("prepare", (ctx, in) -> "base", FAST))
                .then(new ParallelNode("work", List.of(
                        new ParallelBranch("x", "doX", (ctx, in) -> {
                            x.incrementAndGet();
                            return "X";
                        }, FAST),
                        new ParallelBranch("y", "doY", (ctx, in) -> {
                            Thread.sleep(40);
                            y.incrementAndGet();
                            return "Y";
                        }, FAST),
                        new ParallelBranch("z", "doZ", (ctx, in) -> {
                            z.incrementAndGet();
                            if (crashBeforeRestart && ctx.attempt() == 1) {
                                zEntered.countDown();
                                // 第一次执行阻塞直到被杀（模拟崩溃时在途）。
                                new CountDownLatch(1).await(10, TimeUnit.SECONDS);
                                return "Z-late";
                            }
                            return "Z";
                        }, FAST)),
                        "merge",
                        (ctx, results) -> {
                            join.incrementAndGet();
                            return String.join("|", results.outputs());
                        }, FAST))
                .then(new ActivityNode("finish", (ctx, in) -> "done:" + in, FAST))
                .build();
    }

    private StepRecord step(WorkflowInstance instance, String suffix) {
        return instance.getSteps().stream()
                .filter(s -> s.getNodeId().endsWith(suffix))
                .findFirst().orElseThrow(() -> new AssertionError("missing " + suffix));
    }

    @Test
    void completedBranchesAreNotReexecutedAfterRestartAndJoinRunsOnce() throws Exception {
        AtomicInteger x = new AtomicInteger();
        AtomicInteger y = new AtomicInteger();
        AtomicInteger z = new AtomicInteger();
        AtomicInteger join = new AtomicInteger();
        CountDownLatch zEntered = new CountDownLatch(1);

        // 第一次运行：x、y 完成，z 执行中模拟进程崩溃。
        WorkflowEngine engine1 = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        engine1.register(fanoutWorkflow(x, y, z, join, zEntered, true));
        WorkflowInstance started = engine1.start("pfan", "key-recover-parallel", "in");
        assertTrue(zEntered.await(5, TimeUnit.SECONDS), "z 分支应已进入执行");
        // 等待 x、y 落盘完成（它们比 z 快；轮询持久化分支输出）。
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            var outs = engine1.get(started.getId()).orElseThrow().getBranchOutputs().get("work");
            if (outs != null && outs.containsKey("x") && outs.containsKey("y")) {
                break;
            }
            Thread.sleep(10);
        }
        engine1.shutdown(); // 杀掉：z 被中断，未持久化完成
        assertEquals(1, x.get());
        assertEquals(1, y.get());
        assertEquals(1, z.get());

        // 重启：同一目录、同一组计数器（判断是否重复执行）。
        WorkflowEngine engine2 = new WorkflowEngine(new FileWorkflowStore(dataDir), 16, 8);
        try {
            engine2.register(fanoutWorkflow(x, y, z, join, zEntered, true));
            int recovered = engine2.recover();
            assertEquals(1, recovered, "应恢复 1 个未完结实例");

            WorkflowInstance terminal = engine2.awaitTerminal(started.getId(), 10_000);
            assertNotNull(terminal);
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            assertEquals(1, x.get(), "已完成分支 x 重启后不得重跑");
            assertEquals(1, y.get(), "已完成分支 y 重启后不得重跑");
            assertEquals(2, z.get(), "崩溃时在途的 z 应作为新 attempt 补跑一次");
            assertEquals(1, join.get(), "汇合在重启前后总计只能执行一次");
            assertEquals("X|Y|Z", step(terminal, "work/join").getOutput(),
                    "汇合只由补跑结果与既有输出构成，且按定义顺序");
            assertEquals("done:X|Y|Z", step(terminal, "finish").getOutput());
            assertEquals(StepStatus.COMPLETED, step(terminal, "work/p/x").getStatus());
            assertTrue(terminal.getOpenParallels().isEmpty());
        } finally {
            engine2.shutdown();
        }
    }
}
