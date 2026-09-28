package com.github.lisama569.durablefunctionruntimejava.engine;

/** 分支选择失败：谓词抛异常，或没有任何分支命中且缺少兜底分支。属不可重试的定义/数据错误。 */
class BranchSelectionException extends RuntimeException {
    BranchSelectionException(String choiceNodeId, Throwable cause) {
        super("branch selection failed at " + choiceNodeId, cause);
    }
}
