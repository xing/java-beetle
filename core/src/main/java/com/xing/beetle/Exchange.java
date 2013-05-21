package com.xing.beetle;

import java.util.Objects;

/**
 * Beetle AMQP Exchange configuration.
 */
public class Exchange {

    private final String name;
    private final boolean topic;
    private final boolean durable;

    private Exchange(String name, boolean topic, boolean durable) {
        this.name = name;
        this.topic = topic;
        this.durable = durable;
    }

    public String getName() {
        return name;
    }

    public boolean isTopic() {
        return topic;
    }

    public boolean isDurable() {
        return durable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, topic, durable);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Exchange other = (Exchange) obj;
        return Objects.equals(this.name, other.name)
            && Objects.equals(this.topic, other.topic)
            && Objects.equals(this.durable, other.durable);
    }

    @Override
    public String toString() {
        return "Exchange{" +
            "name='" + name + '\'' +
            ", topic=" + topic +
            ", durable=" + durable +
            '}';
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
