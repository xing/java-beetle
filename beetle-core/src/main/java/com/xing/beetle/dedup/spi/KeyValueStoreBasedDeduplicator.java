package com.xing.beetle.dedup.spi;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.dedup.spi.KeyValueStore.Value;

import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/** Deduplicator implementing the status storage methods based on a key value store. */
public class KeyValueStoreBasedDeduplicator implements Deduplicator {

  private KeyValueStore store;
  private BeetleAmqpConfiguration beetleAmqpConfig;

  public KeyValueStoreBasedDeduplicator(
      KeyValueStore store, BeetleAmqpConfiguration beetleAmqpConfig) {
    this.store = requireNonNull(store);
    this.beetleAmqpConfig = requireNonNull(beetleAmqpConfig);
  }

  private String dedupKey(String key, String keySuffix) {
    return key + ":" + keySuffix;
  }

  @Override
  public boolean tryAcquireMutex(String key, int secondsToExpire) {
    return store.putIfAbsentTtl(
        dedupKey(key, MUTEX), new Value(System.currentTimeMillis()), secondsToExpire);
  }

  @Override
  public void releaseMutex(String key) {
    store.delete(dedupKey(key, MUTEX));
  }

  @Override
  public void complete(String key) {
    store.put(dedupKey(key, STATUS), new Value("completed"));
  }

  @Override
  public boolean isComplete(String key) {
    return store
        .get(dedupKey(key, STATUS))
        .map(value -> value.getAsString().equals("completed"))
        .orElse(false);
  }

  @Override
  public void init(String key) {
    store.putIfAbsentTtl(
        dedupKey(key, STATUS),
        new Value("incomplete"),
        beetleAmqpConfig.getBeetleRedisStatusKeyExpiryIntervalSeconds());
  }

  @Override
  public boolean delayed(String key) {
    long now = System.currentTimeMillis();
    return store
        .get(dedupKey(key, DELAY))
        .map(delay -> delay.getAsNumber() > 0 && delay.getAsNumber() > now)
        .orElse(false);
  }

  @Override
  public void setDelay(String key, long timestamp) {
    store.put(dedupKey(key, DELAY), new Value(timestamp));
  }

  @Override
  public long incrementAttempts(String key) {
    return store.increase(dedupKey(key, ATTEMPTS));
  }

  @Override
  public long incrementExceptions(String key) {
    return store.increase(dedupKey(key, EXCEPTIONS));
  }

  @Override
  public long incrementAckCount(String key) {
    return store.increase(dedupKey(key, ACK_COUNT));
  }

  @Override
  public void deleteKeys(String key) {
    Stream<String> suffixStream =
        (beetleAmqpConfig.getBeetleRedisStatusKeyExpiryIntervalSeconds() > 0)
            ? Arrays.stream(keySuffixes).filter(s -> !s.equals(STATUS))
            : Arrays.stream(keySuffixes);
    store.delete(suffixStream.map(s -> dedupKey(key, s)).toArray(String[]::new));
  }

  @Override
  public BeetleAmqpConfiguration getBeetleAmqpConfiguration() {
    return this.beetleAmqpConfig;
  }
}
