package com.xing.beetle.dedup.spi;

import com.xing.beetle.dedup.api.Interruptable;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.util.ExceptionSupport;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * This interface provides an implementation for deduplication logic which also takes care of
 * retries and delaying the handler execution. This implementation depends on a set of methods for
 * storing the status of the messages which must be implemented by the implementing classes.
 */
public interface Deduplicator {

  // key suffixes for tracking the message status
  String MUTEX = "mutex";
  String STATUS = "status";
  String ACK_COUNT = "ack_count";
  String TIMEOUT = "timeout";
  String DELAY = "delay";
  String ATTEMPTS = "attempts";
  String EXCEPTIONS = "exceptions";
  String EXPIRES = "expires";

  String[] keySuffixes =
      new String[] {MUTEX, STATUS, ACK_COUNT, TIMEOUT, DELAY, ATTEMPTS, EXCEPTIONS, EXPIRES};

  boolean tryAcquireMutex(String key, int secondsToExpire);

  void releaseMutex(String key);

  void complete(String key);

  boolean completed(String key);

  boolean delayed(String key);

  void setDelay(String key, long timestamp);

  long incrementAttempts(String key);

  long incrementExceptions(String key);

  long incrementAckCount(String key);

  void deleteKeys(String key);

  DeduplicationConfiguration getConfiguration();

  default <M> void runHandler(
      M message, MessageListener<M> listener, MessageAdapter<M> adapter, Duration timeout) {
    Interruptable<M> interruptable = new Interruptable<>(listener);
    // Schedule an interruption for the execution of the handler when the timeout is expired
    CompletableFuture.delayedExecutor(timeout.toMillis(), TimeUnit.MILLISECONDS)
        .execute(interruptable::interruptTimedOutAndRethrow);
    // actually run the handler, i.e handle the message
    try {
      interruptable.onMessage(message);
    } catch (Throwable throwable) {
      if (throwable.getCause() != null && throwable.getCause() instanceof InterruptedException) {
        listener.onFailure(
            message,
            String.format("Beetle: message handling timed out for %s", adapter.keyOf(message)));
      }
      ExceptionSupport.sneakyThrow(throwable);
    }
  }

  default <M> void handle(M message, MessageAdapter<M> adapter, MessageListener<M> listener) {
    String key = adapter.keyOf(message);
    // check if the message is ancient or it was already completed.
    if (isExpired(message, adapter)) {
      dropMessage(
          message,
          adapter,
          listener,
          String.format("Beetle: ignored expired message %s", adapter.keyOf(message)));
    } else if (completed(key)) {
      dropMessage(
          message,
          adapter,
          listener,
          String.format("Beetle: ignored completed message %s", adapter.keyOf(message)));
    } else {
      if (tryAcquireMutex(key, getConfiguration().getMutexExpiration())) {
        if (completed(key)) {
          dropMessage(
              message,
              adapter,
              listener,
              String.format("Beetle: ignored completed message %s", adapter.keyOf(message)));
        } else if (delayed(key)) {
          adapter.requeue(message);
        } else {
          long attempt = incrementAttempts(key);
          if (attempt >= getConfiguration().getMaxHandlerExecutionAttempts()) {
            failureNotification(
                message,
                adapter,
                listener,
                key,
                String.format(
                    "Beetle: reached the handler execution attempts limit: %d on %s",
                    getConfiguration().getMaxHandlerExecutionAttempts(), adapter.keyOf(message)));
          } else {
            try {
              runHandler(
                  message,
                  listener,
                  adapter,
                  Duration.ofSeconds(getConfiguration().getHandlerTimeout()));
              complete(key);
              cleanUp(message, adapter);
            } catch (Throwable throwable) {
              handleException(message, adapter, listener, key, attempt, throwable);
            } finally {
              releaseMutex(key);
            }
          }
        }
      } else {
        adapter.requeue(message);
      }
    }
  }

  private <M> void dropMessage(
      M message, MessageAdapter<M> adapter, MessageListener<M> listener, String reason) {
    adapter.drop(message);
    listener.onDropped(message, reason);
    cleanUp(message, adapter);
  }

  private <M> boolean isExpired(M message, MessageAdapter<M> adapter) {
    long expiresAt = adapter.expiresAt(message);
    return expiresAt < System.currentTimeMillis();
  }

  private <M> void handleException(
      M message,
      MessageAdapter<M> adapter,
      MessageListener<M> listener,
      String key,
      long attempt,
      Throwable throwable) {
    long exceptions = incrementExceptions(key);
    if (exceptions >= getConfiguration().getExceptionLimit()) {
      failureNotification(
          message,
          adapter,
          listener,
          key,
          String.format(
              "Beetle: reached the handler exceptions limit: %d on %s",
              getConfiguration().getExceptionLimit(), adapter.keyOf(message)));
    } else {
      setDelay(key, System.currentTimeMillis() + nextDelay(attempt));
      adapter.requeue(message);
      // let Spring know about the exception so that it rejects the message
      ExceptionSupport.sneakyThrow(throwable);
    }
  }

  private <M> void failureNotification(
      M message,
      MessageAdapter<M> adapter,
      MessageListener<M> listener,
      String key,
      String reason) {
    complete(key);
    adapter.drop(message);
    listener.onFailure(message, reason);
    cleanUp(message, adapter);
  }

  /**
   * deletes all keys associated with this message in the deduplication store if we are sure this is
   * the last message with this message id.
   */
  private <M> void cleanUp(M message, MessageAdapter<M> adapter) {
    if (getConfiguration().getMaxHandlerExecutionAttempts() > 1 || adapter.isRedundant(message)) {
      if (!adapter.isRedundant(message) || incrementAckCount(adapter.keyOf(message)) >= 2) {
        deleteKeys(adapter.keyOf(message));
      }
    }
  }

  private int nextDelay(long attempt) {
    return (int)
        Math.min(
            getConfiguration().getMaxhandlerExecutionAttemptsDelay(),
            getConfiguration().getHandlerExecutionAttemptsDelay() * Math.pow(2, attempt));
  }
}
