package com.xing.beetle.dedup.spi;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.util.ExceptionSupport;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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

  boolean tryAcquireMutex(String messageId, int secondsToExpire);

  void releaseMutex(String messageId);

  void complete(String messageId);

  boolean completed(String messageId);

  boolean delayed(String messageId);

  void setDelay(String messageId, long timestamp);

  long incrementAttempts(String messageId);

  long incrementExceptions(String messageId);

  long incrementAckCount(String messageId);

  void deleteKeys(String messageId);

  boolean initKeys(String messageId, long expirationTimeSecs);

  BeetleAmqpConfiguration getBeetleAmqpConfiguration();

  default <M> void runHandler(
      M message, MessageListener<M> listener, MessageAdapter<M> adapter, Duration timeout) {
    try {
      CompletableFuture<Void> cf =
          CompletableFuture.runAsync(
              () -> {
                try {
                  listener.onMessage(message);
                } catch (Throwable t) {
                  ExceptionSupport.sneakyThrow(t);
                }
              });
      cf.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (Throwable throwable) {
      if (throwable instanceof InterruptedException
          || (throwable.getCause() != null
              && throwable.getCause() instanceof InterruptedException)) {
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
      if (tryAcquireMutex(key, getBeetleAmqpConfiguration().getMutexExpiration())) {
        initKeys(adapter.keyOf(message), adapter.expiresAt(message));
        if (completed(key)) {
          dropMessage(
              message,
              adapter,
              listener,
              String.format("Beetle: ignored completed message %s", adapter.keyOf(message)));
        } else if (delayed(key)) {
          adapter.requeue(message);
          try {
            listener.onRequeued(message);
          } catch (IOException e) {
            e.printStackTrace();
          }
        } else {
          long attempt = incrementAttempts(key);
          if (attempt > getBeetleAmqpConfiguration().getMaxHandlerExecutionAttempts()) {
            failureNotification(
                message,
                adapter,
                listener,
                String.format(
                    "Beetle: reached the handler execution attempts limit: %d on %s",
                    getBeetleAmqpConfiguration().getMaxHandlerExecutionAttempts(),
                    adapter.keyOf(message)));
          } else {
            try {
              runHandler(
                  message,
                  listener,
                  adapter,
                  Duration.ofSeconds(getBeetleAmqpConfiguration().getHandlerTimeoutSeconds()));
              complete(key);
              cleanUp(message, adapter);
            } catch (Throwable throwable) {
              handleException(message, adapter, listener, attempt, throwable);
            } finally {
              releaseMutex(key);
            }
          }
        }
      } else {
        adapter.requeue(message);
        try {
          listener.onRequeued(message);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  default <M> void dropMessage(
      M message, MessageAdapter<M> adapter, MessageListener<M> listener, String reason) {
    adapter.drop(message);
    listener.onDropped(message, reason);
    cleanUp(message, adapter);
  }

  default <M> boolean isExpired(M message, MessageAdapter<M> adapter) {
    // expires_at is a unix timestamp (so in seconds)
    long expiresAt = adapter.expiresAt(message);
    if (expiresAt <= 0) return false;
    return expiresAt < Instant.now().getEpochSecond();
  }

  default <M> void handleException(
      M message,
      MessageAdapter<M> adapter,
      MessageListener<M> listener,
      long attempt,
      Throwable throwable) {
    long exceptions = incrementExceptions(adapter.keyOf(message));
    if (exceptions >= getBeetleAmqpConfiguration().getExceptionLimit()) {
      failureNotification(
          message,
          adapter,
          listener,
          String.format(
              "Beetle: reached the handler exceptions limit: %d on %s",
              getBeetleAmqpConfiguration().getExceptionLimit(), adapter.keyOf(message)));
    } else {
      setDelay(adapter.keyOf(message), System.currentTimeMillis() + nextDelay(attempt) * 1000);
      adapter.requeue(message);

      if (adapter.shouldNotifyException()) {
        ExceptionSupport.sneakyThrow(throwable);
      }
    }
  }

  default <M> void failureNotification(
      M message, MessageAdapter<M> adapter, MessageListener<M> listener, String reason) {
    complete(adapter.keyOf(message));
    adapter.drop(message);
    listener.onFailure(message, reason);
    cleanUp(message, adapter);
  }

  /**
   * deletes all keys associated with this message in the deduplication store if we are sure this is
   * the last message with this message id.
   */
  default <M> void cleanUp(M message, MessageAdapter<M> adapter) {
    if (!adapter.isRedundant(message) || incrementAckCount(adapter.keyOf(message)) >= 2) {
      deleteKeys(adapter.keyOf(message));
    }
  }

  default int nextDelay(long attempt) {
    return (int)
        Math.min(
            getBeetleAmqpConfiguration().getMaxhandlerExecutionAttemptsDelay(),
            getBeetleAmqpConfiguration().getHandlerExecutionAttemptsDelaySeconds()
                * Math.pow(2, attempt));
  }
}
