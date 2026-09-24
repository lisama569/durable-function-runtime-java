package com.github.lisama569.durablefunctionruntimejava.engine.tests.support;

import java.util.ArrayList;
import java.util.List;

import com.github.lisama569.durablefunctionruntimejava.engine.function.AsyncDispatcher;
import com.github.lisama569.durablefunctionruntimejava.engine.function.AsyncTask;

/**
 * Async dispatcher that does not call anything external: dispatched tasks are
 * captured and completed manually by the test. Dispatch counts are visible so
 * retry behaviour can be asserted.
 */
public class ManualAsync implements AsyncDispatcher {

    public final List<AsyncTask> dispatched = new ArrayList<>();
    public int dispatchCount;

    @Override
    public synchronized void dispatch(AsyncTask task) {
        dispatchCount++;
        dispatched.add(task);
    }

    public synchronized AsyncTask lastTask() {
        return dispatched.get(dispatched.size() - 1);
    }

    public synchronized int size() {
        return dispatched.size();
    }
}
