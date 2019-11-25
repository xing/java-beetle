package com.xing.beetle.dedup.spi;

import com.xing.beetle.dedup.api.MessageListener;

import static java.util.Objects.requireNonNull;

public interface DedupStore {

  class KeyValueStoreBasedDedupStore implements DedupStore {

    private KeyValueStore store;

    public KeyValueStoreBasedDedupStore(KeyValueStore store) {
      this.store = requireNonNull(store);
    }

    @Override
    public Mutex tryAcquireMutex(String key) {
      return null;
    }

    @Override
    public void releaseMutex(String key) {}

    @Override
    public void complete(String key) {
      store.put(key + "_status", new KeyValueStore.Value("completed"));
    }

    @Override
    public boolean completed(String key) {
      KeyValueStore.Value status =
          store.putIfAbsent(key + "_status", new KeyValueStore.Value("incomplete"));
      return status.getAsString().equals("completed");
    }

    @Override
    public long incrementAttempts(String key) {
      return 0;
    }

    @Override
    public long incrementExceptions(String key) {
      return 0;
    }
  }

  class Mutex {

    public Mutex(boolean acquired, long expires) {
      this.acquired = acquired;
      this.expires = expires;
    }

    private final boolean acquired;
    private final long expires;
  }

  long MAX_EXCEPTIONS = 10;
  long MAX_ATTEMPTS = 10;
  long DELAY = 10;
  Long MAX_DELAY = 10L;
  long TIMEOUT = 10;

  Mutex tryAcquireMutex(String key);

  void releaseMutex(String key);

  void complete(String key);

  boolean completed(String key);

  long incrementAttempts(String key);

  long incrementExceptions(String key);

  default <M> void handle(M message, MessageAdapter<M> adapter, MessageListener<M> listener) {
    String key = adapter.keyOf(message);
    long expiresAt = adapter.expiresAt(message);
    if (expiresAt < System.currentTimeMillis()) {
      adapter.acknowledge(message);
      listener.onDropped(message);
    } else if (completed(key)) {
      adapter.acknowledge(message);
    } else {
      Mutex mutex = tryAcquireMutex(key);
      if (mutex.acquired) {
        long attempt = incrementAttempts(key);
        if (attempt >= MAX_ATTEMPTS) {
          complete(key);
          adapter.acknowledge(message);
          listener.onFailure(message);
        } else {
          try {
            listener.onMessage(message);
            complete(key);
            adapter.acknowledge(message);
          } catch (Throwable throwable) {
            long exceptions = incrementExceptions(key);
            if (exceptions >= MAX_EXCEPTIONS) {
              complete(key);
              adapter.acknowledge(message);
              listener.onFailure(message);
            } else {
              adapter.requeue(message);
            }
          } finally {
            releaseMutex(key);
          }
        }
      } else {
        if (mutex.expires < System.currentTimeMillis()) {
          releaseMutex(key);
        }
        adapter.requeue(message);
      }
    }
  }
}
