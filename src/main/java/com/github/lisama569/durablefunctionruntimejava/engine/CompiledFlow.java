package com.github.lisama569.durablefunctionruntimejava.engine;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepHandler;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDef;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ActivityNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ChoiceBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ChoiceNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.JoinHandler;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.ParallelNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.WorkflowNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * 定义编译后的不可变流程图：节点 id、节点信息与同序列内后继关系。
 *
 * <p>id 规则（稳定、定义内唯一）：
 * <ul>
 *   <li>顶层节点：{@code name}；</li>
 *   <li>条件分支体内节点：{@code <choiceId>/b/<branchLabel>/<name>}（逐层叠加）；</li>
 *   <li>并行分支活动：{@code <parallelId>/p/<branchLabel>}；汇合活动：{@code <parallelId>/join}。</li>
 * </ul>
 */
final class CompiledFlow {

    enum NodeKind { ACTIVITY, CHOICE, PARALLEL }

    /** 活动的执行范围：普通顺序活动 / 并行分支活动 / 并行汇合活动。 */
    enum ActivityScope { SEQUENTIAL, PARALLEL_BRANCH, JOIN }

    static final class ActivitySpec {
        final String displayName;
        final StepHandler handler;
        final JoinHandler joinHandler;
        final RetryPolicy retryPolicy;
        final ActivityScope scope;
        /** scope != SEQUENTIAL 时所属并行节点 id。 */
        final String parallelId;
        /** PARALLEL_BRANCH 时的分支标签。 */
        final String branchLabel;

        ActivitySpec(String displayName, StepHandler handler, JoinHandler joinHandler, RetryPolicy retryPolicy,
                     ActivityScope scope, String parallelId, String branchLabel) {
            this.displayName = displayName;
            this.handler = handler;
            this.joinHandler = joinHandler;
            this.retryPolicy = retryPolicy;
            this.scope = scope;
            this.parallelId = parallelId;
            this.branchLabel = branchLabel;
        }
    }

    static final class ChoiceArm {
        final String label;
        final Predicate<String> when; // null 表示兜底分支
        final List<String> childIds;

        ChoiceArm(String label, Predicate<String> when, List<String> childIds) {
            this.label = label;
            this.when = when;
            this.childIds = childIds;
        }

        boolean isFallback() {
            return when == null;
        }
    }

    static final class ParallelArm {
        final String label;
        final String activityId;

        ParallelArm(String label, String activityId) {
            this.label = label;
            this.activityId = activityId;
        }
    }

    static final class NodeInfo {
        final String id;
        final String name;
        final NodeKind kind;
        /** ACTIVITY：活动规格；否则 null。 */
        final ActivitySpec activity;
        /** CHOICE：分支列表（定义顺序）；否则 null。 */
        final List<ChoiceArm> choiceArms;
        /** PARALLEL：分支列表（定义顺序）；否则 null。 */
        final List<ParallelArm> parallelArms;
        /** PARALLEL：汇合活动 id；否则 null。 */
        final String joinId;
        /** 同序列（顶层或某条分支体）内的后继节点 id；序列末尾为 null。 */
        String successorId;
        final String parentId;

        NodeInfo(String id, String name, NodeKind kind, ActivitySpec activity,
                 List<ChoiceArm> choiceArms, List<ParallelArm> parallelArms,
                 String joinId, String parentId, String successorId) {
            this.id = id;
            this.name = name;
            this.kind = kind;
            this.activity = activity;
            this.choiceArms = choiceArms;
            this.parallelArms = parallelArms;
            this.joinId = joinId;
            this.parentId = parentId;
            this.successorId = successorId;
        }
    }

    private final String name;
    private final List<String> rootIds;
    private final Map<String, NodeInfo> nodes = new java.util.LinkedHashMap<>();
    /** 按稳定顺序排列的全部活动 id（用于预创建步骤记录、跳过标记）。 */
    private final List<String> allActivityIds = new ArrayList<>();

    private CompiledFlow(String name) {
        this.name = name;
        this.rootIds = new ArrayList<>();
    }

    static CompiledFlow compile(WorkflowDef def) {
        CompiledFlow flow = new CompiledFlow(def.name());
        // 顶层序列：id 前缀与 parentId 均为 null。
        flow.compileSequence(def.nodes(), null, null, flow.rootIds, null);
        return flow;
    }

    String name() {
        return name;
    }

    List<String> rootIds() {
        return rootIds;
    }

    NodeInfo node(String id) {
        NodeInfo info = nodes.get(id);
        if (info == null) {
            throw new IllegalStateException("unknown node id: " + id + " in workflow " + name);
        }
        return info;
    }

    List<String> allActivityIds() {
        return allActivityIds;
    }

    /** 收集某节点子树下的全部活动 id（活动自身 / 条件各分支 / 并行分支与汇合）。 */
    List<String> descendantActivityIds(String nodeId) {
        List<String> out = new ArrayList<>();
        collectActivities(node(nodeId), out);
        return out;
    }

    private void collectActivities(NodeInfo info, List<String> out) {
        switch (info.kind) {
            case ACTIVITY -> out.add(info.id);
            case CHOICE -> {
                for (ChoiceArm arm : info.choiceArms) {
                    for (String childId : arm.childIds) {
                        collectActivities(node(childId), out);
                    }
                }
            }
            case PARALLEL -> {
                for (ParallelArm arm : info.parallelArms) {
                    out.add(arm.activityId);
                }
                out.add(info.joinId);
            }
        }
    }

    /**
     * 编译一个节点序列：节点 id 顺序写入 sink，同序列内相邻链接，序列末尾后继为 afterId。
     *
     * @param idPrefix 节点 id 的路径前缀（如分支臂 route/b/A）；顶层序列为 null
     * @param parentId 外层控制节点 id（分支体即选择节点 id）；顶层序列为 null
     */
    private void compileSequence(List<WorkflowNode> sequence, String idPrefix, String parentId,
                                 List<String> sink, String afterId) {
        List<String> ids = new ArrayList<>();
        for (WorkflowNode node : sequence) {
            ids.add(compileNode(node, idPrefix, parentId, afterId));
        }
        for (int i = 0; i < ids.size(); i++) {
            nodes.get(ids.get(i)).successorId = i + 1 < ids.size() ? ids.get(i + 1) : afterId;
            sink.add(ids.get(i));
        }
    }

    /**
     * @param idPrefix 节点 id 路径前缀
     * @param parentId 外层控制节点 id
     * @param afterId  该节点所在序列结束后应继续的节点 id（供内部分支体继承）
     */
    private String compileNode(WorkflowNode node, String idPrefix, String parentId, String afterId) {
        if (node instanceof ActivityNode a) {
            String id = idPrefix == null ? a.name() : idPrefix + "/" + a.name();
            registerActivity(id, a.name(), a.handler(), null, a.retryPolicy(),
                    ActivityScope.SEQUENTIAL, null, null, parentId, afterId);
            return id;
        }
        if (node instanceof ChoiceNode c) {
            String id = idPrefix == null ? c.name() : idPrefix + "/" + c.name();
            List<ChoiceArm> arms = new ArrayList<>();
            int fallbacks = 0;
            List<String> armLabels = new ArrayList<>();
            for (ChoiceBranch branch : c.branches()) {
                if (!armLabels.add(branch.label())) {
                    throw new IllegalArgumentException("duplicate choice branch label: " + branch.label());
                }
                if (branch.isFallback()) {
                    fallbacks++;
                }
                List<String> childIds = new ArrayList<>();
                // 分支体节点：id 路径带臂标识，但 parentId 指向选择节点本身（汇合后继用）。
                compileSequence(branch.body(), id + "/b/" + branch.label(), id, childIds, afterId);
                arms.add(new ChoiceArm(branch.label(), branch.when(), childIds));
            }
            if (fallbacks > 1) {
                throw new IllegalArgumentException("choice " + id + " has more than one fallback branch");
            }
            NodeInfo info = new NodeInfo(id, c.name(), NodeKind.CHOICE, null,
                    List.copyOf(arms), null, null, parentId, afterId);
            register(info);
            return id;
        }
        if (node instanceof ParallelNode p) {
            String id = idPrefix == null ? p.name() : idPrefix + "/" + p.name();
            List<ParallelArm> arms = new ArrayList<>();
            List<String> labels = new ArrayList<>();
            for (ParallelBranch branch : p.branches()) {
                if (!labels.add(branch.label())) {
                    throw new IllegalArgumentException("duplicate parallel branch label: " + branch.label());
                }
                String activityId = id + "/p/" + branch.label();
                registerActivity(activityId, branch.activityName(), branch.handler(), null,
                        branch.retryPolicy(), ActivityScope.PARALLEL_BRANCH, id, branch.label(),
                        id, null);
                arms.add(new ParallelArm(branch.label(), activityId));
            }
            String joinId = id + "/join";
            registerActivity(joinId, p.joinName(), null, p.joinHandler(), p.joinRetryPolicy(),
                    ActivityScope.JOIN, id, null, id, afterId);
            NodeInfo info = new NodeInfo(id, p.name(), NodeKind.PARALLEL, null,
                    null, List.copyOf(arms), joinId, parentId, afterId);
            register(info);
            return id;
        }
        throw new IllegalArgumentException("unsupported workflow node: " + node.getClass());
    }

    private void registerActivity(String id, String displayName, StepHandler handler, JoinHandler joinHandler,
                                  RetryPolicy retryPolicy, ActivityScope scope, String parallelId,
                                  String branchLabel, String parentId, String successorId) {
        ActivitySpec spec = new ActivitySpec(displayName, handler, joinHandler, retryPolicy,
                scope, parallelId, branchLabel);
        NodeInfo info = new NodeInfo(id, displayName, NodeKind.ACTIVITY, spec,
                null, null, null, parentId, successorId);
        register(info);
        allActivityIds.add(id);
    }

    private void register(NodeInfo info) {
        if (nodes.putIfAbsent(info.id, info) != null) {
            throw new IllegalArgumentException("duplicate node id in workflow " + name + ": " + info.id);
        }
    }
}
