package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

import java.util.concurrent.atomic.AtomicInteger;

enum ChannelReadMode {
    GET {
        @Override
        int read(Channel channel, String queue, MessageAcknowledgementStrategy strategy, long expectedNumberOfMessages) throws Exception {
            int messageCount = 0;
            GetResponse msg;
            while ((msg = channel.basicGet(queue, strategy.isAuto())) != null) {
                messageCount++;
                strategy.ack(channel, msg.getEnvelope().getDeliveryTag(), expectedNumberOfMessages);
            }
            return messageCount;
        }
    }, CONSUME {
        @Override
        int read(Channel channel, String queue, MessageAcknowledgementStrategy strategy, long expectedNumberOfMessages) throws Exception {
            AtomicInteger messageCount = new AtomicInteger();
            String consumerTag = channel.basicConsume(queue, strategy.isAuto(), (tag, msg) -> {
                messageCount.incrementAndGet();
                strategy.ack(channel, msg.getEnvelope().getDeliveryTag(), expectedNumberOfMessages);
            }, System.err::println);
            Thread.sleep(1000);
            channel.basicCancel(consumerTag);
            return messageCount.get();
        }
    };

    abstract int read(Channel channel, String queue, MessageAcknowledgementStrategy strategy, long expectedNumberOfMessages) throws Exception;
}
