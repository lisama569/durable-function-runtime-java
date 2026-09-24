package com.github.lisama569.durablefunctionruntimejava.engine;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Local-file configuration for the durable engine. */
@ConfigurationProperties(prefix = "durable.engine")
public class EngineProperties {

    /** Directory holding instance state, event journal and side effects. */
    private String storageDir = "target/durable-state";

    /** Background tick interval (ms) driving timeouts and retry backoff. */
    private long tickIntervalMs = 200L;

    /** Start the background ticker / recovery on Spring startup. */
    private boolean autoStart = true;

    public String getStorageDir() { return storageDir; }
    public void setStorageDir(String storageDir) { this.storageDir = storageDir; }
    public long getTickIntervalMs() { return tickIntervalMs; }
    public void setTickIntervalMs(long tickIntervalMs) { this.tickIntervalMs = tickIntervalMs; }
    public boolean isAutoStart() { return autoStart; }
    public void setAutoStart(boolean autoStart) { this.autoStart = autoStart; }
}
