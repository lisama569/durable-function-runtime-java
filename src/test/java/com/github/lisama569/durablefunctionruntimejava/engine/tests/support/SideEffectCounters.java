package com.github.lisama569.durablefunctionruntimejava.engine.tests.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared counters for "external" side effects used by test workflows.
 * Because business steps route effects through {@code runOnce}, these
 * counters must move at most once per (instance, step) no matter how many
 * retries or restarts happen.
 */
public final class SideEffectCounters {

    private static final Map<String, AtomicInteger> EXECUTIONS = new ConcurrentHashMap<>();

    private SideEffectCounters() {
    }

    public static AtomicInteger of(String name) {
        return EXECUTIONS.computeIfAbsent(name, k -> new AtomicInteger());
    }

    public static int executions(String name) {
        return of(name).get();
    }

    public static void reset() {
        EXECUTIONS.clear();
    }
}
