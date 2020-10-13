package com.xing.beetle.dedup.spi;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleMessageAdapter;
import com.xing.beetle.dedup.api.Interruptable;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.util.ExceptionSupport;

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

  BeetleAmqpConfiguration getBeetleAmqpConfiguration();

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
      if (throwable instanceof InterruptedException
          || (throwable.getCause() != null
              && throwable.getCause() instanceof InterruptedException)) {
        listener.onFailure(
            message,
            String.format("Beetle: message handling timed out for %s", adapter.messageId(message)));
      }
      ExceptionSupport.sneakyThrow(throwable);
    }
  }

  default <M> void handle(
      M message, String queueName, MessageAdapter<M> adapter, MessageListener<M> listener) {

    if (!adapter.isRedundant(message)) {
      runHandler(
          message,
          listener,
          adapter,
          Duration.ofSeconds(getBeetleAmqpConfiguration().getHandlerTimeoutSeconds()));
      return;
    }

    String key = "msgid:" + queueName + ":" + adapter.messageId(message);
    // check if the message is ancient or it was already completed.
    if (isExpired(message, adapter)) {
      dropMessage(
          message,
          key,
          adapter,
          listener,
          String.format("Beetle: ignored expired message %s", key));
    } else if (completed(key)) {
      dropMessage(
          message,
          key,
          adapter,
          listener,
          String.format("Beetle: ignored completed message %s", key));
    } else {
      deduplicate(message, adapter, listener, key);
    }
  }

  private <M> void deduplicate(
      M message, MessageAdapter<M> adapter, MessageListener<M> listener, String key) {
    if (tryAcquireMutex(key, getBeetleAmqpConfiguration().getMutexExpiration())) {
      if (completed(key)) {
        dropMessage(
            message,
            key,
            adapter,
            listener,
            String.format("Beetle: ignored completed message %s", key));
      } else if (delayed(key)) {
        adapter.requeue(message);
        listener.onRequeued(message);
      } else {
        long attempt = incrementAttempts(key);
        if (attempt > getBeetleAmqpConfiguration().getMaxHandlerExecutionAttempts()) {
          failureNotification(
              message,
              key,
              adapter,
              listener,
              String.format(
                  "Beetle: reached the handler execution attempts limit: %d on %s",
                  getBeetleAmqpConfiguration().getMaxHandlerExecutionAttempts(), key));
        } else {
          try {
            runHandler(
                message,
                listener,
                adapter,
                Duration.ofSeconds(getBeetleAmqpConfiguration().getHandlerTimeoutSeconds()));
            complete(key);
            cleanUp(message, key, adapter);
          } catch (Throwable throwable) {
            handleException(message, key, adapter, listener, attempt, throwable);
          } finally {
            releaseMutex(key);
          }
        }
      }
    } else {
      adapter.requeue(message);
      listener.onRequeued(message);
    }
  }

  private <M> void dropMessage(
      M message,
      String key,
      MessageAdapter<M> adapter,
      MessageListener<M> listener,
      String reason) {
    adapter.drop(message);
    listener.onDropped(message, reason);
    cleanUp(message, key, adapter);
  }

  private <M> boolean isExpired(M message, MessageAdapter<M> adapter) {
    // expires_at is a unix timestamp (so in seconds)
    long expiresAt = adapter.expiresAt(message);
    if (expiresAt <= 0) return false;
    return expiresAt < Instant.now().getEpochSecond();
  }

  private <M> void handleException(
      M message,
      String key,
      MessageAdapter<M> adapter,
      MessageListener<M> listener,
      long attempt,
      Throwable throwable) {
    long exceptions = incrementExceptions(key);
    if (exceptions >= getBeetleAmqpConfiguration().getExceptionLimit()) {
      failureNotification(
          message,
          key,
          adapter,
          listener,
          String.format(
              "Beetle: reached the handler exceptions limit: %d on %s",
              getBeetleAmqpConfiguration().getExceptionLimit(), key));
    } else {
      setDelay(key, System.currentTimeMillis() + nextDelay(attempt) * 1000);
      adapter.requeue(message);

      if (!(adapter instanceof BeetleMessageAdapter)) {
        // let Spring know about the exception so that it rejects the message
        ExceptionSupport.sneakyThrow(throwable);
      }
    }
  }

  private <M> void failureNotification(
      M message,
      String key,
      MessageAdapter<M> adapter,
      MessageListener<M> listener,
      String reason) {
    complete(key);
    adapter.drop(message);
    listener.onFailure(message, reason);
    cleanUp(message, key, adapter);
  }

  /**
   * deletes all keys associated with this message in the deduplication store if we are sure this is
   * the last message with this message id.
   */
  private <M> void cleanUp(M message, String key, MessageAdapter<M> adapter) {
    if (getBeetleAmqpConfiguration().getMaxHandlerExecutionAttempts() > 1
        || adapter.isRedundant(message)) {
      if (!adapter.isRedundant(message) || incrementAckCount(key) >= 2) {
        deleteKeys(key);
      }
    }
  }

  private int nextDelay(long attempt) {
    return (int)
        Math.min(
            getBeetleAmqpConfiguration().getMaxhandlerExecutionAttemptsDelay(),
            getBeetleAmqpConfiguration().getHandlerExecutionAttemptsDelaySeconds()
                * Math.pow(2, attempt));
  }
}
