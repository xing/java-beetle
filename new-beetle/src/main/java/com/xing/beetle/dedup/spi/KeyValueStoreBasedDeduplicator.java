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

  private String key(String messageId, String keySuffix) {
    return messageId + ":" + keySuffix;
  }

  @Override
  public boolean tryAcquireMutex(String messageId, int secondsToExpire) {
    return store.putIfAbsentTtl(
        key(messageId, MUTEX), new Value(System.currentTimeMillis()), secondsToExpire);
  }

  @Override
  public void releaseMutex(String messageId) {
    store.delete(key(messageId, MUTEX));
  }

  @Override
  public void complete(String messageId) {
    store.put(key(messageId, STATUS), new Value("completed"));
  }

  @Override
  public boolean completed(String messageId) {
    if (store.putIfAbsentTtl(
        key(messageId, STATUS),
        new Value("incomplete"),
        beetleAmqpConfig.getBeetleRedisStatusKeyExpiryInterval())) {
      return false;
    } else {
      return store
          .get(key(messageId, STATUS))
          .map(value -> value.getAsString().equals("completed"))
          .orElse(false);
    }
  }

  @Override
  public boolean delayed(String messageId) {
    return store
        .get(key(messageId, DELAY))
        .map(delay -> delay.getAsNumber() > 0 && delay.getAsNumber() > System.currentTimeMillis())
        .orElse(false);
  }

  @Override
  public void setDelay(String messageId, long timestamp) {
    store.put(key(messageId, DELAY), new Value(timestamp));
  }

  @Override
  public long incrementAttempts(String messageId) {
    return store.increase(key(messageId, ATTEMPTS));
  }

  @Override
  public long incrementExceptions(String messageId) {
    return store.increase(key(messageId, EXCEPTIONS));
  }

  @Override
  public long incrementAckCount(String messageId) {
    return store.increase(key(messageId, ACK_COUNT));
  }

  @Override
  public void deleteKeys(String messageId) {
    Stream<String> suffixStream =
        (beetleAmqpConfig.getBeetleRedisStatusKeyExpiryInterval() > 0)
            ? Arrays.stream(keySuffixes).filter(s -> !s.equals(STATUS))
            : Arrays.stream(keySuffixes);
    store.delete(suffixStream.map(s -> key(messageId, s)).toArray(String[]::new));
  }

  @Override
  public BeetleAmqpConfiguration getBeetleAmqpConfiguration() {
    return this.beetleAmqpConfig;
  }
}
