package com.xing.beetle;

import static com.xing.beetle.Util.currentTimeSeconds;

public class HandlerStatus {

    private final String status;
    private final long timeout;
    private final long attempts;
    private final long exceptions;
    private final long delay;

    public HandlerStatus(String status, String timeout, String attempts, String exceptions, String delay) {
        this.status = status;
        this.timeout = Long.valueOf(timeout != null ? timeout : "0");
        this.attempts = Long.valueOf(attempts != null ? attempts : "0");
        this.exceptions = Long.valueOf(exceptions != null ? exceptions : "0");
        this.delay = Long.valueOf(delay != null ? delay : "0");
    }

    public boolean isCompleted() {
        return status.equals("complete");
    }

    public boolean isTimedOut() {
        return timeout > currentTimeSeconds();
    }

    public boolean shouldDelay() {
        return delay > currentTimeSeconds();
    }

    public long getAttempts() {
        return attempts;
    }

    public long getExceptions() {
        return exceptions;
    }
}
