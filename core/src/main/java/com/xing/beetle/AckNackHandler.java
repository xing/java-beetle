package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AckNackHandler implements Runnable {
	
	private static Logger log = LoggerFactory.getLogger(AckNackHandler.class);

	private final Client client;
	
	public AckNackHandler(Client client) {
		this.client = client;
	}
	
    @Override
    public void run() {
        while (client.isRunning()) {
            HandlerResponse response;
            MessageInfo messageInfo = null;
            Channel channel = null;
            try {
            	/*
            	 * pollForHandlerResponse() is actually polling, not taking. It will return null every few milliseconds
            	 * so we can be shut down at some point without blocking forever.
            	 */
                final Future<HandlerResponse> handlerResponseFuture = client.pollForHandlerResponse();
                if (handlerResponseFuture == null) {
                    // nothing to do yet.
                    continue;
                }

                // retrieve the routingkey and deliverytag associated with the future.
                // FIXME this isn't particularly nice, if we had a custom Future implementation, then that could hold the data.
                messageInfo = client.takeMessageInfo(handlerResponseFuture);
                if (messageInfo == null) {
                    log.error("Unknown handler response object, this should never happen. Ignoring response.");
                    continue;
                }
                channel = messageInfo.getChannel();
                response = handlerResponseFuture.get();
                if (response.isSuccess()) {
                    final Connection connection = channel.getConnection();
                    log.debug("ACKing message from delivery tag {} on channel {} broker {}:{}",
                        messageInfo.getDeliveryTag(), channel.getChannelNumber(), connection.getAddress(), connection.getPort());
                    channel.basicAck(messageInfo.getDeliveryTag(), false);
                }
            } catch (InterruptedException ignored) {
            } catch (ExecutionException e) {
                // TODO make decision whether to requeue or not
                try {
                    log.debug("NACKing message from routing key {}", messageInfo.getRoutingKey());
                    channel.basicNack(messageInfo.getDeliveryTag(), false, true);
                } catch (IOException e1) {
                    log.error("Could not send NACK to broker in response to handler result.", e1);
                }
            } catch (IOException e) {
                log.error("Could not send ACK to broker in response to handler result.", e);
            }

        }
    }
}