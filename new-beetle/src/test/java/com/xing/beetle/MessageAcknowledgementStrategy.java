package com.xing.beetle;

import com.rabbitmq.client.Channel;

import java.io.IOException;

enum MessageAcknowledgementStrategy {
    AUTO {
        @Override
        void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages) throws IOException {
        }
    }, SINGLE {
        @Override
        void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages) throws IOException {
            channel.basicAck(deliveryTag, false);
        }
    }, MULTIPLE {
        @Override
        void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages) throws IOException {
            if (deliveryTag == expectedNumberOfMessages) {
                channel.basicAck(deliveryTag, true);
            }
        }
    };

    abstract void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages) throws IOException;

    boolean isAuto() {
        return this == AUTO;
    }
}
