package com.xing.beetle.dedup.spi;

import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.KeyValueStore.Value;
import com.xing.beetle.util.ExceptionSupport;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public interface Deduplicator {

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

  class KeyValueStoreBasedDeduplicator implements Deduplicator {

    private KeyValueStore store;
    private DeduplicationConfiguration configuration;

    public KeyValueStoreBasedDeduplicator(KeyValueStore store) {
      this.store = requireNonNull(store);
      this.configuration = new DeduplicationConfiguration();
    }

    public KeyValueStoreBasedDeduplicator(
        KeyValueStore store, DeduplicationConfiguration configuration) {
      this.store = requireNonNull(store);
      this.configuration = requireNonNull(configuration);
    }

    String key(String messageId, String keySuffix) {
      return messageId + ":" + keySuffix;
    }

    @Override
    public boolean tryAcquireMutex(String key, int secondsToExpire) {
      return store.putIfAbsentTtl(
          key(key, MUTEX), new Value(System.currentTimeMillis()), secondsToExpire);
    }

    @Override
    public void releaseMutex(String key) {
      store.remove(key(key, MUTEX));
    }

    @Override
    public void complete(String key) {
      store.put(key(key, STATUS), new KeyValueStore.Value("completed"));
    }

    @Override
    public boolean completed(String key) {
      KeyValueStore.Value status =
          store.putIfAbsent(key(key, STATUS), new KeyValueStore.Value("incomplete"));
      return status.getAsString().equals("completed");
    }

    @Override
    public boolean delayed(String key) {
      return store
          .get(key(key, DELAY))
          .map(delay -> delay.getAsNumber() > 0 && delay.getAsNumber() > System.currentTimeMillis())
          .orElse(false);
    }

    @Override
    public void setDelay(String key, long timestamp) {
      store.put(key(key, DELAY), new Value(timestamp));
    }

    @Override
    public long incrementAttempts(String key) {
      return store.increase(key(key, ATTEMPTS));
    }

    @Override
    public long incrementExceptions(String key) {
      return store.increase(key(key, EXCEPTIONS));
    }

    @Override
    public long incrementAckCount(String key) {
      return store.increase(key(key, ACK_COUNT));
    }

    @Override
    public void deleteKeys(String key) {
      for (String keySuffix : keySuffixes) {
        store.delete(key(key, keySuffix));
      }
    }

    @Override
    public DeduplicationConfiguration getConfiguration() {
      return this.configuration;
    }
  }

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

  default <M> void runHandler(M message, MessageListener<M> listener, Duration timeout) {
    MessageListener.Interruptable<M> interruptable = new MessageListener.Interruptable<>(listener);
    CompletableFuture.delayedExecutor(timeout.toMillis(), TimeUnit.MILLISECONDS)
        .execute(interruptable::interruptTimedOutAndRethrow);
    try {
      interruptable.onMessage(message);
    } catch (Throwable throwable) {
      ExceptionSupport.sneakyThrow(throwable);
    }
  }

  default <M> void handle(M message, MessageAdapter<M> adapter, MessageListener<M> listener) {
    String key = adapter.keyOf(message);
    // check if the message is ancient or not.
    if (isExpired(message, adapter)) {
      dropMessage(message, adapter, listener);
    } else if (completed(key)) {
      dropMessage(message, adapter, listener);
    } else {
      if (tryAcquireMutex(key, getConfiguration().getMutexExpiration())) {
        if (completed(key)) {
          dropMessage(message, adapter, listener);
        } else if (delayed(key)) {
          adapter.requeue(message);
        } else {
          long attempt = incrementAttempts(key);
          if (attempt >= getConfiguration().getMaxHandlerExecutionAttempts()) {
            failureNotification(message, adapter, listener, key);
          } else {
            try {
              // run handler
              runHandler(
                  message, listener, Duration.ofSeconds(getConfiguration().getHandlerTimeout()));
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

  private <M> void dropMessage(M message, MessageAdapter<M> adapter, MessageListener<M> listener) {
    adapter.drop(message);
    listener.onDropped(message);
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
      failureNotification(message, adapter, listener, key);
    } else {
      setDelay(key, System.currentTimeMillis() + nextDelay(attempt));
      adapter.requeue(message);
      // let Spring know about the exception so that it rejects the message
      ExceptionSupport.sneakyThrow(throwable);
    }
  }

  private <M> void failureNotification(
      M message, MessageAdapter<M> adapter, MessageListener<M> listener, String key) {
    complete(key);
    adapter.drop(message);
    listener.onFailure(message);
    cleanUp(message, adapter);
  }

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
