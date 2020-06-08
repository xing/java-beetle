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
      System.out.println(
          "HANDLE FAILED CALLED ON "
              + this.hashCode()
              + " thread "
              + current.getId()
              + " delegate "
              + delegate.hashCode());
      return delegate.handleFailed(exception, attempt);
    } finally {
      System.out.println("CURRENT SET TO NUlL " + this.hashCode() + " thread " + current.getId());
      current = null;
    }
  }

  public void interruptTimedOutAndRethrow() {
    if (current != null) {
      System.out.println("INTERRUPT CALLED ON " + this.hashCode() + " thread " + current.getId());
      current.interrupt();
    } else {
      System.out.println("CURRENT IS NULL " + this.hashCode());
    }
  }

  @Override
  public void onDropped(M message, String reason) {
    try {
      current = Thread.currentThread();
      System.out.println(
          "ON DROPPED CALLED ON "
              + this.hashCode()
              + " thread "
              + current.getId()
              + " delegate "
              + delegate.hashCode());
      delegate.onDropped(message, reason);
    } finally {
      System.out.println("CURRENT SET TO NUlL " + this.hashCode() + " thread " + current.getId());
      current = null;
    }
  }

  @Override
  public void onMessage(M message) throws Throwable {
    try {
      current = Thread.currentThread();
      System.out.println(
          "ON MESSAGE CALLED ON "
              + this.hashCode()
              + " thread "
              + current.getId()
              + " delegate "
              + delegate.hashCode());
      delegate.onMessage(message);
    } finally {
      System.out.println("CURRENT SET TO NUlL " + this.hashCode() + " thread " + current.getId());
      current = null;
    }
  }
}
