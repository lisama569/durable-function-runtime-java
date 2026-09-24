package com.github.lisama569.durablefunctionruntimejava.engine.function;

/**
 * Dispatches an async step. Implementations must trigger the external work
 * (or no-op on duplicate dispatch) and eventually call back the engine with
 * the completion token. The step stays WAITING until then.
 */
@FunctionalInterface
public interface AsyncDispatcher {

    void dispatch(AsyncTask task) throws Exception;
}
