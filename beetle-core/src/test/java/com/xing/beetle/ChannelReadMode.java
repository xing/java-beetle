package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.util.concurrent.atomic.AtomicInteger;

enum ChannelReadMode {
  GET {
    @Override
    int readAck(
        Channel channel,
        String queue,
        MessageAcknowledgementStrategy strategy,
        long expectedNumberOfMessages)
        throws Exception {
      int messageCount = 0;
      GetResponse msg;
      while ((msg = channel.basicGet(queue, strategy.isAuto())) != null) {
        messageCount++;
        strategy.ack(channel, msg.getEnvelope().getDeliveryTag(), expectedNumberOfMessages);
      }
      return messageCount;
    }

    @Override
    int readNack(
        Channel channel,
        String queue,
        MessageAcknowledgementStrategy strategy,
        boolean requeue,
        long numberOfMessagesToNack)
        throws Exception {
      int messageCount = 0;
      GetResponse msg;
      while ((msg = channel.basicGet(queue, strategy.isAuto())) != null) {
        messageCount++;
        if (messageCount > numberOfMessagesToNack) {
          channel.basicAck(msg.getEnvelope().getDeliveryTag(), true);
        } else {
          strategy.nack(
              channel, msg.getEnvelope().getDeliveryTag(), requeue, numberOfMessagesToNack);
        }
      }
      return messageCount;
    }
  },
  CONSUME {
    @Override
    int readAck(
        Channel channel,
        String queue,
        MessageAcknowledgementStrategy strategy,
        long expectedNumberOfMessages)
        throws Exception {
      AtomicInteger messageCount = new AtomicInteger();
      String consumerTag =
          channel.basicConsume(
              queue,
              strategy.isAuto(),
              (tag, msg) -> {
                messageCount.incrementAndGet();
                strategy.ack(channel, msg.getEnvelope().getDeliveryTag(), expectedNumberOfMessages);
              },
              System.err::println);
      Thread.sleep(1000);
      channel.basicCancel(consumerTag);
      return messageCount.get();
    }

    @Override
    int readNack(
        Channel channel,
        String queue,
        MessageAcknowledgementStrategy strategy,
        boolean requeue,
        long numberOfMessagesToNack)
        throws Exception {
      AtomicInteger messageCount = new AtomicInteger();
      String consumerTag =
          channel.basicConsume(
              queue,
              strategy.isAuto(),
              (tag, msg) -> {
                messageCount.incrementAndGet();
                if (requeue && messageCount.get() > numberOfMessagesToNack) {
                  channel.basicAck(msg.getEnvelope().getDeliveryTag(), false);
                } else {
                  strategy.nack(
                      channel, msg.getEnvelope().getDeliveryTag(), requeue, numberOfMessagesToNack);
                }
              },
              System.err::println);
      Thread.sleep(1000);
      channel.basicCancel(consumerTag);
      return messageCount.get();
    }
  };

  abstract int readAck(
      Channel channel,
      String queue,
      MessageAcknowledgementStrategy strategy,
      long expectedNumberOfMessages)
      throws Exception;

  abstract int readNack(
      Channel channel,
      String queue,
      MessageAcknowledgementStrategy strategy,
      boolean requeue,
      long numberOfMessagesToNack)
      throws Exception;
}
