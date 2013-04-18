package com.xing.beetle;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

/**
 *
 */
public class BeetleConsumer extends DefaultConsumer {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Client client;
    protected final Channel subscriberChannel;
    protected final MessageHandler handler;

    public BeetleConsumer(Client client, Channel subscriberChannel, MessageHandler handler) {
        super(subscriberChannel);

        this.client = client;
        this.subscriberChannel = subscriberChannel;
        this.handler = handler;
    }

    @Override
    public void handleDelivery(String consumerTag, final Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

        final Callable<HandlerResponse> handlerProcessor = handler.process(envelope, properties, body);

        try {
            final Runnable handlerTask = new Runnable() {
                @Override
                public void run() {
                    try {
                        HandlerResponse response = handlerProcessor.call();
                        if (response.isSuccess()) {
                            final Connection connection = subscriberChannel.getConnection();
                            log.debug("ACKing message from delivery tag {} on channel {} broker {}:{}",
                                envelope.getDeliveryTag(), subscriberChannel.getChannelNumber(), connection.getAddress(), connection.getPort());

                            subscriberChannel.basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            try {
                                subscriberChannel.basicNack(envelope.getDeliveryTag(), false, true);
                            } catch (IOException e1) {
                                log.error("Could not NACK message.", e1);
                            }
                        }
                    } catch (Exception e) {
                        try {
                            subscriberChannel.basicNack(envelope.getDeliveryTag(), false, true);
                        } catch (IOException e1) {
                            log.error("Could not NACK message.", e1);
                        }
                    }
                }
            };

            client.submit(handlerTask);

        } catch (RejectedExecutionException e) {
            log.error("Could not submit message processor to executor! Requeueing message.", e);
            subscriberChannel.basicNack(envelope.getDeliveryTag(), false, true);
        }
    }
}
