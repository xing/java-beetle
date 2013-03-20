package com.xing.beetle;

/**
 *
 */
public class Queue {
    private final String name;
    private final String key;
    private final String queueNameOnBroker;
    private final String exchangeName;
    private final boolean autoDelete;

    public Queue(String name, String key, String queueNameOnBroker, String exchangeName, boolean autoDelete) {
        this.name = name;
        this.key = key;
        this.queueNameOnBroker = queueNameOnBroker;
        this.exchangeName = exchangeName;
        this.autoDelete = autoDelete;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private String queueNameOnBroker;
        private String key;
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

        public Builder autoDelete(boolean autoDelete) {
            this.autoDelete = autoDelete;
            return this;
        }

        public Queue build() {
            return new Queue(name, key, queueNameOnBroker, exchangeName, autoDelete);
        }
    }
}
