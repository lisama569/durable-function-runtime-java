package com.github.lisama569.durablefunctionruntimejava.workflow;

/**
 * 分支选择器：在事件循环线程同步执行，依据上游任务输出决定走哪个分支。
 * 返回值为被选分支的 label；返回 null / 未知 label 视为定义错误，实例立即失败。
 */
@FunctionalInterface
public interface BranchSelector {
    String select(String upstreamOutput) throws Exception;
}
