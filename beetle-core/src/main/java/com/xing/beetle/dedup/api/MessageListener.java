package com.xing.beetle.dedup.api;

import com.rabbitmq.client.Delivery;
import org.springframework.amqp.core.Message;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/**
 * Interface for listening the (AMQP) messages and handling the different cases including message
 * delivery, failures and dropped messages.
 */
@FunctionalInterface
public interface MessageListener<M> {

  default boolean handleFailed(Throwable exception, int attempt) {
    logger().log(Level.WARNING, "Beetle message processing failed due to: {0}", exception);
    return true;
  }

  default Logger logger() {
    String loggerName = getClass().getName();
    String canonical = getClass().getCanonicalName();
    if (canonical != null) {
      int offset = canonical.indexOf("$$Lambda$");
      loggerName = offset > 0 ? canonical.substring(0, offset) : canonical;
    }
    return System.getLogger(loggerName);
  }

  default void onDropped(M message, String reason) {
    logger().log(Level.WARNING, reason);
  }

  default void onRequeued(M message) throws IOException {
    if (message instanceof Message) {
      Message m = (Message) message;
      logger().log(Level.WARNING, m.getMessageProperties().getMessageId() + " requeued");
      return;
    }

    if (message instanceof Delivery) {
      Delivery m = (Delivery) message;
      logger().log(Level.WARNING, m.getProperties().getMessageId() + " requeued");
      return;
    }

    logger().log(Level.WARNING, message + " requeued");
  }

  default void onFailure(M message, String reason) {
    logger().log(Level.WARNING, reason);
  }

  void onMessage(M message) throws Throwable;
}
