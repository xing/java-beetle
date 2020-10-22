package com.xing.beetle.dedup.api;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Interface for listening the (AMQP) messages and handling the different cases including message
 * delivery, failures and dropped messages.
 */
@FunctionalInterface
public interface MessageListener<M> {

  default boolean handleFailed(Throwable exception, int attempt) {
    logger().log(Level.SEVERE, "Beetle message processing failed due to: {0}", exception);
    return true;
  }

  default Logger logger() {
    String loggerName = getClass().getName();
    String canonical = getClass().getCanonicalName();
    if (canonical != null) {
      int offset = canonical.indexOf("$$Lambda$");
      loggerName = offset > 0 ? canonical.substring(0, offset) : canonical;
    }
    return Logger.getLogger(loggerName);
  }

  default void onDropped(M message, String reason) {
    logger().log(Level.WARNING, reason);
  }

  default void onRequeued(M message) throws IOException {}

  default void onFailure(M message, String reason) {
    logger().log(Level.WARNING, reason);
  }

  void onMessage(M message) throws Throwable;
}
