package com.xing.beetle;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A Beetle AMQP queue configuration.
 */
public class Message {

    private final String name;
    private final String key;
    private final Exchange exchange;
    private final boolean redundant;
    private final long duration;
    private final TimeUnit timeUnit;

    private Message(String name, String key, Exchange exchange, boolean redundant, long duration, TimeUnit timeUnit) {
        this.name = name;
        this.key = key;
        this.exchange = exchange;
        this.redundant = redundant;
        this.duration = duration;
        this.timeUnit = timeUnit;
    }

    public String getName() {
        return name;
    }

    public String getKey() {
        return key;
    }

    public Exchange getExchange() {
        return exchange;
    }

    public boolean isRedundant() {
        return redundant;
    }

    public long getDuration() {
        return duration;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public static Message.Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, key, exchange, redundant, duration, timeUnit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Message other = (Message) obj;
        return Objects.equals(this.name, other.name)
            && Objects.equals(this.key, other.key)
            && Objects.equals(this.exchange, other.exchange)
            && Objects.equals(this.redundant, other.redundant)
            && Objects.equals(this.duration, other.duration)
            && Objects.equals(this.timeUnit, other.timeUnit);
    }

    @Override
    public String toString() {
        return "Message{" +
            "name='" + name + '\'' +
            ", key='" + key + '\'' +
            ", exchange=" + exchange +
            ", redundant=" + redundant +
            ", duration=" + duration +
            ", timeUnit=" + timeUnit +
            '}';
    }

    public static class Builder {

        private String name;
        private String key;
        private String exchangeName;

        private boolean redundant = false;
        private long duration = 1;
        private TimeUnit timeUnit = TimeUnit.DAYS;
        private Exchange exchange;

        /**
         * The logical name of the queue you are declaring. Use something that makes sense in your application.
         * <p>
         * Also sets {@link #exchange(String)} and {@link #key(String)} by default. You can override those properties
         * by just calling the setters.
         *
         * @param name logical queue name
         * @return the builder instance
         */
        public Builder name(String name) {
            this.name = name;
            if (exchangeName != null) {
                exchange(name);
            }
            if (key != null) {
                key(name);
            }
            return this;
        }

        /**
         * Sets the routing key of this queue, used for subscribing.
         *
         * @see #name(String)
         * @param key the routing key
         * @return the builder instance
         */
        public Builder key(String key) {
            this.key = key;
            return this;
        }

        /**
         * Sets the exchange name of this queue, used for publishing.
         *
         * @param exchangeName the exchange this queue is published to
         * @return the builder instance
         * @see #name(String)
         */
        public Builder exchange(String exchangeName) {
            this.exchangeName = exchangeName;
            return this;
        }

        public Builder exchange(Exchange exchange) {
            this.exchange = exchange;
            return this;
        }
        
        /**
         * Whether to publish this queue in a redundant fashion to at least two brokers.
         * <p>
         * Publishing non-redundantly with failover to a secondary broker is the default.
         *
         * @param redundant true means redundant, false means publish with failover
         * @return the builder instance
         */
        public Builder redundant(boolean redundant) {
            this.redundant = redundant;
            return this;
        }

        /**
         * Sets the TTL for published messages on the broker(s).
         * <p>
         * Defaults to 1 day.
         *
         * @param duration time duration, default 1
         * @param timeUnit time unit, default day.
         * @return the builder instance
         */
        public Builder ttl(long duration, TimeUnit timeUnit) {
            this.duration = duration;
            this.timeUnit = timeUnit;
            return this;
        }

        /**
         * Builds an immutable Message object.
         *
         * @return new Message object
         */
        public Message build() {
            if (name == null) {
                throw new IllegalStateException("No name given for the Beetle queue.");
            }
            if (exchange == null) {
                exchange = Exchange.builder().name(exchangeName).build();
            }
            return new Message(name, key, exchange, redundant, duration, timeUnit);
        }
    }
}
