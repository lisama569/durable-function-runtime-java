package com.github.lisama569.durablefunctionruntimejava.web;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ActivityNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ChoiceBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ChoiceNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelNode;
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
            @Value("${workflow.max-branches-per-instance:16}") int maxBranchesPerInstance,
            @Value("${workflow.max-parallel-in-flight:8}") int maxParallelInFlight) {
        FileWorkflowStore store = new FileWorkflowStore(Path.of(dataDir));
        WorkflowEngine engine = new WorkflowEngine(store, maxBranchesPerInstance, maxParallelInFlight);

        // 旧版纯串行示例：校验 -> 扣款 -> 发货，行为与升级前完全一致。
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

        // 新版示例：风控分级 -> （VIP 走专属通道 / 普通走标准通道 / 兜底人工审核）
        //          -> 并行执行 通知、积分、记账 三路，汇合后归档。
        engine.register(com.github.lisama569.durablefunctionruntimejava.workflow.ControlFlowDefinition
                .name("fulfillmentWorkflow")
                .then(new ActivityNode("classify", (ctx, input) -> {
                    log.info("[instance={}] step=classify input={}", ctx.instanceId(), input);
                    return input;
                }, RetryPolicy.defaults()))
                .then(new ChoiceNode("channel", List.of(
                        new ChoiceBranch("vip", "vip"::equals, List.of(
                                new ActivityNode("vipChannel", (ctx, input) -> "vip-handled:" + input,
                                        RetryPolicy.defaults()))),
                        new ChoiceBranch("standard", "standard"::equals, List.of(
                                new ActivityNode("standardChannel", (ctx, input) -> "std-handled:" + input,
                                        RetryPolicy.defaults()))),
                        new ChoiceBranch("manual", null, List.of(
                                new ActivityNode("manualReview", (ctx, input) -> "manual-handled:" + input,
                                        RetryPolicy.defaults()))))))
                .then(new ParallelNode("settle", List.of(
                        new ParallelBranch("notify", "sendNotification",
                                (ctx, input) -> "notified:" + input, RetryPolicy.defaults()),
                        new ParallelBranch("points", "grantPoints",
                                (ctx, input) -> "points:" + input, RetryPolicy.defaults()),
                        new ParallelBranch("ledger", "writeLedger",
                                (ctx, input) -> "ledger:" + input,
                                new RetryPolicy(5, 100, 2_000, 10_000))),
                        "aggregate",
                        (ctx, results) -> "aggregated[" + String.join(",", results.outputs()) + "]",
                        RetryPolicy.defaults()))
                .then(new ActivityNode("archive", (ctx, input) -> "archived:" + input, RetryPolicy.defaults()))
                .build());

        int recovered = engine.recover();
        log.info("engine started (maxBranchesPerInstance={}, maxParallelInFlight={}), recovered {} running instance(s) from {}",
                maxBranchesPerInstance, maxParallelInFlight, recovered, dataDir);
        return engine;
    }
}
