package com.xing.beetle.dedup.api;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface MessageListener<M> {

  class Interruptable<M> implements MessageListener<M> {

    private final MessageListener<M> delegate;
    private volatile Thread current;

    public Interruptable(MessageListener<M> delegate) {
      this.delegate = requireNonNull(delegate);
    }

    @Override
    public boolean handleFailed(Throwable exception, int attempt) {
      try {
        current = Thread.currentThread();
        return delegate.handleFailed(exception, attempt);
      } finally {
        current = null;
      }
    }

    public void interruptTimedOutAndRethrow() {
      if (current != null) {
        current.interrupt();
      }
    }

    @Override
    public void onDropped(M message) {
      try {
        current = Thread.currentThread();
        delegate.onDropped(message);
      } finally {
        current = null;
      }
    }

    @Override
    public void onMessage(M message) throws Throwable {
      try {
        current = Thread.currentThread();
        delegate.onMessage(message);
      } finally {
        current = null;
      }
    }
  }

  default boolean handleFailed(Throwable exception, int attempt) {
    logger().log(Level.WARNING, "Beetle message processing failed due to: {0}", exception);
    return true;
  }

  default Logger logger() {
    String canonical = getClass().getCanonicalName();
    int offset = canonical.indexOf("$$Lambda$");
    String loggerName = offset > 0 ? canonical.substring(0, offset) : canonical;
    return System.getLogger(loggerName);
  }

  default void onDropped(M message) {
    logger().log(Level.WARNING, "Beetle dropped already acknowledged message: {0}", message);
  }

  default void onFailure(M message) {
    logger().log(Level.WARNING, "Beetle dropped failed message: {0}", message);
  }

  void onMessage(M message) throws Throwable;
}
