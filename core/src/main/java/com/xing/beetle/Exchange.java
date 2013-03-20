package com.xing.beetle;

/**
 *
 */
public class Exchange {
    private final String name;
    private final boolean topic;
    private final boolean durable;

    public Exchange(String name, boolean topic, boolean durable) {
        this.name = name;
        this.topic = topic;
        this.durable = durable;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private boolean topic = true;
        private boolean durable = true;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder topic(boolean topic) {
            this.topic = topic;
            return this;
        }

        public Builder durable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public Exchange build() {
            if (name == null) {
                throw new IllegalStateException("No name given for the exchange.");
            }
            return new Exchange(name, topic, durable);
        }
    }
}
