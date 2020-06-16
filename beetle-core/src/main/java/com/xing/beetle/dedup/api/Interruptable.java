package com.xing.beetle.dedup.api;

import static java.util.Objects.requireNonNull;

/**
 * Provides an implementation for MessageListener where it is possible to interrupt the message
 * handler execution.
 */
public class Interruptable<M> implements MessageListener<M> {

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
  public void onDropped(M message, String reason) {
    try {
      current = Thread.currentThread();
      delegate.onDropped(message, reason);
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
