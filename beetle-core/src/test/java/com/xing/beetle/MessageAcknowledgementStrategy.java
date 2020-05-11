package com.xing.beetle;

import com.rabbitmq.client.Channel;
import java.io.IOException;

enum MessageAcknowledgementStrategy {
  AUTO {
    @Override
    void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages) throws IOException {}

    @Override
    void nack(Channel channel, long deliveryTag, boolean requeue, long expectedNumberOfMessages)
        throws IOException {}
  },
  SINGLE {
    @Override
    void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages) throws IOException {
      channel.basicAck(deliveryTag, false);
    }

    @Override
    void nack(Channel channel, long deliveryTag, boolean requeue, long expectedNumberOfMessages)
        throws IOException {
      channel.basicNack(deliveryTag, false, requeue);
    }
  },
  MULTIPLE {
    @Override
    void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages) throws IOException {
      if (deliveryTag == expectedNumberOfMessages) {
        channel.basicAck(deliveryTag, true);
      }
    }

    @Override
    void nack(Channel channel, long deliveryTag, boolean requeue, long expectedNumberOfMessages)
        throws IOException {
      if (deliveryTag == expectedNumberOfMessages) {
        channel.basicNack(deliveryTag, true, requeue);
      }
    }
  };

  abstract void ack(Channel channel, long deliveryTag, long expectedNumberOfMessages)
      throws IOException;

  abstract void nack(
      Channel channel, long deliveryTag, boolean requeue, long expectedNumberOfMessages)
      throws IOException;

  boolean isAuto() {
    return this == AUTO;
  }
}
