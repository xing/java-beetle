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

public abstract class MessageHandler {

    private final Logger log = LoggerFactory.getLogger(MessageHandler.class);

	private ConcurrentHashMap<Channel, String> channels = new ConcurrentHashMap<>();

    public abstract Callable<HandlerResponse> process(Envelope envelope, AMQP.BasicProperties properties, byte[] body);

    public void pause() {
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
    }

    public void addChannel(Channel subscriberChannel, String consumerTag) {
        channels.put(subscriberChannel, consumerTag);
    }
}
