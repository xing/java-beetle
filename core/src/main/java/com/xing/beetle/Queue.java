package com.xing.beetle;

import java.util.Objects;

/**
 * A Beetle AMQP Queue configuration.
 */
public class Queue {
    private final String name;
    private final String key;
    private final String queueNameOnBroker;
    private final Exchange exchange;
    private final boolean autoDelete;

    public Queue(String name, String key, String queueNameOnBroker, Exchange exchange, boolean autoDelete) {
        this.name = name;
        this.key = key;
        this.queueNameOnBroker = queueNameOnBroker;
        this.exchange = exchange;
        this.autoDelete = autoDelete;
    }

    public String getName() {
        return name;
    }

    public String getKey() {
        return key;
    }

    public String getQueueNameOnBroker() {
        return queueNameOnBroker;
    }

    public String getExchangeName() {
        return exchange.getName();
    }

    public Exchange getExchange() {
        return exchange;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, key, queueNameOnBroker, exchange, autoDelete);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Queue other = (Queue) obj;
        return Objects.equals(this.name, other.name)
            && Objects.equals(this.key, other.key)
            && Objects.equals(this.queueNameOnBroker, other.queueNameOnBroker)
            && Objects.equals(this.exchange, other.exchange)
            && Objects.equals(this.autoDelete, other.autoDelete);
    }

    public static class Builder {
        private String name;
        private String queueNameOnBroker;
        private String key;
        private Exchange exchange;
        private String exchangeName;
        private boolean autoDelete = false;

        public Builder name(String name) {
            this.name = name;
            if (key == null) {
                key(name);
            }
            if (queueNameOnBroker == null) {
                amqpName(name);
            }
            if (exchangeName == null) {
                exchange(name);
            }
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder amqpName(String queueNameOnBroker) {
            this.queueNameOnBroker = queueNameOnBroker;
            return this;
        }

        public Builder exchange(String exchangeName) {
            this.exchangeName = exchangeName;
            return this;
        }

        public Builder exchange(Exchange exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder autoDelete(boolean autoDelete) {
            this.autoDelete = autoDelete;
            return this;
        }

        public Queue build() {
            if (exchange == null) {
                exchange = Exchange.builder().name(exchangeName).build();
            }
            return new Queue(name, key, queueNameOnBroker, exchange, autoDelete);
        }
    }
}
