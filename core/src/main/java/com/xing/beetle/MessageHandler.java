package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class MessageHandler {

    private final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private final ConcurrentHashMap<Channel, String> channels = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock rwChannelLock = new ReentrantReadWriteLock();

    public abstract Callable<HandlerResponse> doProcess(Envelope envelope, AMQP.BasicProperties properties, byte[] body);

    public Callable<HandlerResponse> process(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {

        final Callable<HandlerResponse> callable = doProcess(envelope, properties, body);
        return new Callable<HandlerResponse>() {
            @Override
            public HandlerResponse call() throws Exception {
                final HandlerResponse response;
                rwChannelLock.readLock().lock();
                if (channels.isEmpty()) {
                    // happens when pause() call was executed in between calling doProcess() and acquiring the read lock.
                    // there's nothing to do that case, because the broker already knows that messages in delivery won't be processed by this consumer.
                     return HandlerResponse.noOp();
                }
                try {
                    response = callable.call();
                } finally {
                    rwChannelLock.readLock().unlock();
                }
                return response;
            }
        };
    }

    public void pause() {
        try {
            if (! rwChannelLock.writeLock().tryLock()) {
                rwChannelLock.writeLock().lock();
            }
            for (Map.Entry<Channel, String> entry : channels.entrySet()) {
                final Channel channel = entry.getKey();
                final String consumerTag = entry.getValue();

                try {
                    log.info("Canceling consumer {} on {}", consumerTag, channel);
                    channel.basicCancel(consumerTag);
                } catch (IOException e) {
                    log.warn("Could not pause channel.", e);
                }
            }
            channels.clear();
        } finally {
            rwChannelLock.writeLock().unlock();
        }
    }

    public void addChannel(Channel subscriberChannel, String consumerTag) {
        channels.put(subscriberChannel, consumerTag);
    }
}
