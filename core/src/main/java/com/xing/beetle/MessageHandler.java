package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public abstract class MessageHandler {
	
    private final Logger log;

	private Set<Channel> channels = new HashSet<>(); // TODO: synchronized? (yolo)
	private String consumerTag;

    public MessageHandler() {
    	log = LoggerFactory.getLogger(this.getClass());
    }
	
    public abstract Callable<HandlerResponse> process(Envelope envelope, AMQP.BasicProperties properties, byte[] body);

    public void pause() {
    	for (Channel channel : channels) {
    		try {
				channel.basicCancel(consumerTag);
				channel.close();
			} catch (IOException e) {
				log.warn("Could not pause channel.", e);
			}
    	}
    	
    	channels.clear();
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public String getConsumerTag() {
        return consumerTag;
    }
}
