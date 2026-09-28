package com.github.lisama569.durablefunctionruntimejava.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * 实例的执行位置状态（持久化，与步骤结果分离）。
 *
 * <p>同一时刻顶层只有一个节点在推进：rootPc 指向下一个待处理顶层节点；
 * 选择分支时记录选中分支与其内游标；并行扇出时为每条未完成分支保留一个游标。</p>
 */
public class ExecutionState {

    /** 顶层节点游标：下一个待处理节点的序号。 */
    private int rootPc;
    /** 当前激活的分支选择节点序号；-1 表示未激活。 */
    private int activeChoiceNode = -1;
    /** 选中分支在该 ChoiceNode 中的序号。 */
    private int activeChoiceBranch = -1;
    /** 选中分支内下一个待执行任务的序号。 */
    private int choiceTaskPc;
    /** 当前激活的并行节点序号；-1 表示未激活。 */
    private int activeParallelNode = -1;
    /** 并行节点中尚未汇合的分支游标；已完成的分支状态置为 COMPLETED 但保留输出于步骤记录。 */
    private List<ParallelCursor> parallelCursors = new ArrayList<>();

    public int getRootPc() { return rootPc; }
    public void setRootPc(int rootPc) { this.rootPc = rootPc; }
    public int getActiveChoiceNode() { return activeChoiceNode; }
    public void setActiveChoiceNode(int activeChoiceNode) { this.activeChoiceNode = activeChoiceNode; }
    public int getActiveChoiceBranch() { return activeChoiceBranch; }
    public void setActiveChoiceBranch(int activeChoiceBranch) { this.activeChoiceBranch = activeChoiceBranch; }
    public int getChoiceTaskPc() { return choiceTaskPc; }
    public void setChoiceTaskPc(int choiceTaskPc) { this.choiceTaskPc = choiceTaskPc; }
    public int getActiveParallelNode() { return activeParallelNode; }
    public void setActiveParallelNode(int activeParallelNode) { this.activeParallelNode = activeParallelNode; }
    public List<ParallelCursor> getParallelCursors() { return parallelCursors; }
    public void setParallelCursors(List<ParallelCursor> parallelCursors) { this.parallelCursors = parallelCursors; }

    public static ExecutionState fresh() {
        return new ExecutionState();
    }
}
