package com.xing.beetle.dedup.spi;

import com.xing.beetle.dedup.spi.KeyValueStore.Value;

import static java.util.Objects.requireNonNull;

/** Deduplicator implementing the status storage methods based on a key value store. */
public class KeyValueStoreBasedDeduplicator implements Deduplicator {

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

  private String key(String messageId, String keySuffix) {
    return messageId + ":" + keySuffix;
  }

  @Override
  public boolean tryAcquireMutex(String key, int secondsToExpire) {
    return store.putIfAbsentTtl(
        key(key, MUTEX), new Value(System.currentTimeMillis()), secondsToExpire);
  }

  @Override
  public void releaseMutex(String key) {
    store.delete(key(key, MUTEX));
  }

  @Override
  public void complete(String key) {
    store.put(key(key, STATUS), new Value("completed"));
  }

  @Override
  public boolean completed(String key) {
    Value status = store.putIfAbsent(key(key, STATUS), new Value("incomplete"));
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
