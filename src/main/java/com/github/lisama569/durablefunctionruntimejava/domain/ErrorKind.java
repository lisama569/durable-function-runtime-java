package com.github.lisama569.durablefunctionruntimejava.domain;

/** 失败原因分类：决定重试语义与终态归因。 */
public enum ErrorKind {
    RETRYABLE,
    NON_RETRYABLE,
    TIMEOUT
}
