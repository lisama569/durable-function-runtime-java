package com.github.lisama569.durablefunctionruntimejava.web;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.ChoiceNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.ParallelNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.TaskNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

@Configuration
public class EngineConfig {

    private static final Logger log = LoggerFactory.getLogger(EngineConfig.class);

    @Bean(destroyMethod = "shutdown")
    public WorkflowEngine workflowEngine(
            @Value("${workflow.data-dir:./workflow-data}") String dataDir,
            @Value("${workflow.worker-threads:16}") int workerThreads,
            @Value("${workflow.max-parallel-branches:8}") int maxParallelBranches) {
        FileWorkflowStore store = new FileWorkflowStore(Path.of(dataDir));
        WorkflowEngine engine = new WorkflowEngine(store, workerThreads, maxParallelBranches);

        // 既有纯串行示例：校验 -> 扣款 -> 发货，语义完全不变（向后兼容）。
        engine.register(new WorkflowDefinition("orderWorkflow", List.of(
                new StepDefinition("validate", (ctx, input) -> {
                    log.info("[instance={}] step=validate idemKey={} input={}", ctx.instanceId(), ctx.idempotencyKey(), input);
                    return "validated:" + input;
                }, RetryPolicy.defaults()),
                new StepDefinition("charge", (ctx, input) -> {
                    log.info("[instance={}] step=charge idemKey={} input={}", ctx.instanceId(), ctx.idempotencyKey(), input);
                    return "charged:" + input;
                }, new RetryPolicy(3, 200, 2_000, 10_000)),
                new StepDefinition("ship", (ctx, input) -> {
                    log.info("[instance={}] step=ship idemKey={} input={}", ctx.instanceId(), ctx.idempotencyKey(), input);
                    return "shipped:" + input;
                }, RetryPolicy.defaults())
        )));

        // 分支 + 并行示例：
        // classify 决定走 express 还是 standard；之后并行 备货+通知 两路，汇合后归档。
        engine.registerFlow(new FlowDefinition("orderFlowV2", List.of(
                new TaskNode("classify", (ctx, input) -> {
                    String tier = input.contains("premium") ? "express" : "standard";
                    log.info("[instance={}] task=classify tier={}", ctx.instanceId(), tier);
                    return tier + ":" + input;
                }, RetryPolicy.defaults()),
                new ChoiceNode("shipping", out -> out.startsWith("express") ? "express" : "standard", List.of(
                        new FlowBranch("express", List.of(
                                new TaskNode("expressPick", (ctx, input) -> "express-packed", RetryPolicy.defaults()))),
                        new FlowBranch("standard", List.of(
                                new TaskNode("standardPick", (ctx, input) -> "standard-packed", RetryPolicy.defaults()))))),
                new ParallelNode("notify", List.of(
                        new FlowBranch("reserve", List.of(
                                new TaskNode("reserve", (ctx, input) -> "reserved", RetryPolicy.defaults()))),
                        new FlowBranch("message", List.of(
                                new TaskNode("sendMessage", (ctx, input) -> "notified", RetryPolicy.defaults())))),
                        (joinCtx, outputs) -> {
                            String joined = String.join("+", outputs);
                            log.info("[instance={}] joined notify branches: {}", joinCtx.instanceId(), joined);
                            return joined;
                        }),
                new TaskNode("archive", (ctx, input) -> "archived(" + input + ")", RetryPolicy.defaults())
        )));

        int recovered = engine.recover();
        log.info("engine started (workerThreads={}, maxParallelBranches={}), recovered {} running instance(s) from {}",
                workerThreads, maxParallelBranches, recovered, dataDir);
        return engine;
    }
}
