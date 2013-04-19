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
        final long deliveryTag = envelope.getDeliveryTag();

        final String messageId = properties.getMessageId();
        final String expires_at = properties.getHeaders().get("expires_at").toString();
        Long ttl = 0L;
        if (expires_at != null) {
            ttl = Long.valueOf(expires_at);
        }
        // check for outdated,expired message
        if ((System.currentTimeMillis() / 1000L) > ttl) {
            log.warn("NACK expired message id {} on {}", messageId, subscriberChannel);
            discardMessage(deliveryTag);
            return;
        }

        if (! client.shouldProcessMessage(subscriberChannel, deliveryTag, messageId)) {
            return;
        }

        final Callable<HandlerResponse> handlerProcessor = handler.process(envelope, properties, body);

        try {
            final Runnable handlerTask = new Runnable() {
                @Override
                public void run() {
                    try {
                        HandlerResponse response = handlerProcessor.call();
                        if (response.isSuccess()) {
                            client.markMessageAsCompleted(messageId);
                            acknowledgeMessage(deliveryTag);
                        } else {
                            // cannot happen right now. delete?
                            processMessageAgainLater(deliveryTag);
                        }
                    } catch (Exception e) {
                        final long exceptions = client.incrementExceptions(messageId);
                        final long attempts = client.getAttemptsCount(messageId);
                        // TODO read exceptions/attempts limit from declared Message!
                        if (exceptions > 1 || attempts > 1) {
                            // exceeded configured exception count
                            log.warn("NACK message attempt or exception count exceeded. {} of {} attempts, {} of {} exceptions",
                                new long[] {attempts, 1, exceptions, 1});
                            discardMessage(deliveryTag);
                        } else {
                            client.removeMessageHandlerLock(messageId);
                            processMessageAgainLater(deliveryTag);
                        }
                    }
                }
            };

            client.submit(handlerTask);

        } catch (RejectedExecutionException e) {
            log.error("Could not submit message processor to executor! Requeueing message.", e);
            processMessageAgainLater(deliveryTag);
        }
    }

    private void acknowledgeMessage(long deliveryTag) {
        try {
            subscriberChannel.basicAck(deliveryTag, false);
        } catch (IOException e) {
            log.error("Could not ACK message", e);
        }
    }

    private void discardMessage(long deliveryTag) {
        nackSafely(deliveryTag, false);
    }

    private void processMessageAgainLater(long deliveryTag) {
        nackSafely(deliveryTag, true);
    }

    private void nackSafely(long deliveryTag, boolean requeue) {
        try {
            subscriberChannel.basicNack(deliveryTag, false, requeue);
        } catch (IOException e) {
            log.error("Could not NACK message.", e);
        }
    }
}
