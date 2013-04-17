package com.xing.beetle;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageInfo {
	
	private static Logger log = LoggerFactory.getLogger(MessageInfo.class);

    private final Channel channel;
    private final long deliveryTag;
	private final String routingKey;

    public MessageInfo(Channel channel, long deliveryTag, String routingKey) {
        this.channel = channel;
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
        return channel;
	}
	
}
