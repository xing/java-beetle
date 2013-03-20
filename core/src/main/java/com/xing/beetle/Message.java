package com.xing.beetle;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Message {

    private final String name;
    private final String key;
    private final String exchange;
    private final boolean redundant;
    private final long duration;
    private final TimeUnit timeUnit;

    public Message(String name, String key, String exchange, boolean redundant, long duration, TimeUnit timeUnit) {

        this.name = name;
        this.key = key;
        this.exchange = exchange;
        this.redundant = redundant;
        this.duration = duration;
        this.timeUnit = timeUnit;
    }

    public static Message.Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private String key;
        private String exchange;

        private boolean redundant = false;
        private long duration = 1;
        private TimeUnit timeUnit = TimeUnit.DAYS;

        public Builder name(String name) {
            this.name = name;
            if (exchange != null) {
                exchange(name);
            }
            if (key != null) {
                key(name);
            }
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder redundant(boolean redundant) {
            this.redundant = redundant;
            return this;
        }

        public Builder ttl(long duration, TimeUnit timeUnit) {
            this.duration = duration;
            this.timeUnit = timeUnit;
            return this;
        }

        public Message build() {
            if (name == null) {
                throw new IllegalStateException("No name given for the Beetle message.");
            }
            return new Message(name, key, exchange, redundant, duration, timeUnit);
        }
    }
}
