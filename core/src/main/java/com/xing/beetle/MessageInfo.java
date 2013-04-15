package com.xing.beetle;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;

public class MessageInfo {
	
	private static Logger log = LoggerFactory.getLogger(MessageInfo.class);
	
	private final long deliveryTag;
	private final String routingKey;
	private BeetleChannels beetleChannels;
	
	public MessageInfo(BeetleChannels beetleChannels, long deliveryTag, String routingKey) {
	    this.beetleChannels = beetleChannels;
	    this.deliveryTag = deliveryTag;
	    this.routingKey = routingKey;
	}
	
	public long getDeliveryTag() {
	    return deliveryTag;
	}
	
	public String getRoutingKey() {
	    return routingKey;
	}
	
	public Channel getChannel() {
	    try {
	        return beetleChannels.getPublisherChannel();
	    } catch (IOException e) {
	        log.error("Cannot create publisher channel!", e);
	    }
	    return null;
	}
	
}
