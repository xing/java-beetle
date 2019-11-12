package com.xing.beetle;

import static com.xing.beetle.Util.currentTimeSeconds;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class BeetleConsumer extends DefaultConsumer {

  private final Logger log = LoggerFactory.getLogger(this.getClass());

  private final Client client;
  private final Channel subscriberChannel;
  private final ConsumerConfiguration config;

  public BeetleConsumer(Client client, Channel subscriberChannel, ConsumerConfiguration config) {
    super(subscriberChannel);

    this.client = client;
    this.subscriberChannel = subscriberChannel;
    this.config = config;
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

  @Override
  public void handleDelivery(
      String consumerTag, final Envelope envelope, AMQP.BasicProperties properties, byte[] body)
      throws IOException {
    final long deliveryTag = envelope.getDeliveryTag();

    final String messageId = properties.getMessageId();
    final String expires_at = properties.getHeaders().get("expires_at").toString();
    Long ttl = 0L;
    if (expires_at != null) {
      ttl = Long.valueOf(expires_at);
    }
    // check for outdated,expired message
    if (currentTimeSeconds() > ttl) {
      log.warn("NACK expired message id {} on {}", messageId, subscriberChannel);
      discardMessage(deliveryTag);
      return;
    }

    if (!client.shouldProcessMessage(subscriberChannel, deliveryTag, messageId, config)) {
      return;
    }

    final Callable<HandlerResponse> handlerProcessor =
        config.getHandler().process(envelope, properties, body);

    try {
      final Runnable handlerTask =
          new Runnable() {
            @Override
            public void run() {
              try {
                HandlerResponse response = handlerProcessor.call();
                if (response.isSuccess()) {
                  client.markMessageAsCompleted(messageId);
                  acknowledgeMessage(deliveryTag);
                } else if (response.isNoOp()) {
                  // completely ignore this response, happens when pause was called after a handler
                  // started already.
                  log.debug(
                      "Ignoring message processing result because this consumer was already canceled. The message will be redelivered later (or to a different subscriber).");
                } else {
                  // cannot happen right now. delete?
                  processMessageAgainLater(deliveryTag);
                }
              } catch (Exception e) {
                log.debug("Message {}: handler threw an exception", messageId);
                final long exceptions = client.incrementExceptions(messageId);
                final long attempts = client.getAttemptsCount(messageId);
                if (exceptions >= config.getExceptions() || attempts >= config.getAttempts()) {
                  // exceeded configured exception count
                  log.warn(
                      "NACK message {} attempt or exception count exceeded. {} of {} attempts, {} of {} exceptions",
                      messageId,
                      attempts,
                      config.getAttempts(),
                      exceptions,
                      config.getExceptions());
                  discardMessage(deliveryTag);
                } else {
                  client.removeMessageHandlerLock(messageId, config);
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

  private void nackSafely(long deliveryTag, boolean requeue) {
    try {
      subscriberChannel.basicNack(deliveryTag, false, requeue);
    } catch (IOException e) {
      log.error("Could not NACK message.", e);
    }
  }

  private void processMessageAgainLater(long deliveryTag) {
    nackSafely(deliveryTag, true);
  }
}
