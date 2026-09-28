package com.github.lisama569.durablefunctionruntimejava.workflow.node;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 并行汇合时各分支的结果视图：按分支定义顺序排列，与完成先后无关。
 */
public final class BranchResults {

    private final Map<String, String> byLabel;

    private BranchResults(Map<String, String> byLabel) {
        this.byLabel = byLabel;
    }

    /** 按定义顺序构造（引擎内部使用）。 */
    public static BranchResults inDefinitionOrder(List<String> labels, List<String> outputs) {
        if (labels.size() != outputs.size()) {
            throw new IllegalArgumentException("labels/outputs size mismatch");
        }
        Map<String, String> ordered = new LinkedHashMap<>();
        for (int i = 0; i < labels.size(); i++) {
            ordered.put(labels.get(i), outputs.get(i));
        }
        return new BranchResults(Collections.unmodifiableMap(ordered));
    }

    /** 分支数量。 */
    public int size() {
        return byLabel.size();
    }

    /** 按分支标签取结果。 */
    public String get(String label) {
        return byLabel.get(label);
    }

    /** 按定义顺序的分支标签。 */
    public List<String> labels() {
        return List.copyOf(byLabel.keySet());
    }

    /** 按定义顺序的分支输出。 */
    public List<String> outputs() {
        return List.copyOf(byLabel.values());
    }
}
