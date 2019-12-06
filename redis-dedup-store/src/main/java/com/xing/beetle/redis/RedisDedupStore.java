package com.xing.beetle.redis;

import com.xing.beetle.dedup.spi.KeyValueStore;

import java.util.Optional;

/**
 * KeyValueStore based on Redis providing necessary functionality for message status tracking for
 * deduplication. The communication with Redis is with failover logic which uses retries and
 * timeout.
 */
public class RedisDedupStore implements KeyValueStore {

  private static final Long MUTEX_ACQUIRED = 1L;

  private final Redis redis;
  private final Failover failover;

  RedisDedupStore(BeetleRedisProperties properties) {
    this.redis = new Redis(properties);
    this.failover =
        new Failover(
            properties.getRedisFailoverTimeout(),
            properties.getRedisConfigurationMasterRetries(),
            properties.getRedisConfigurationMasterRetryInterval());
  }

  @Override
  public Optional<Value> get(String key) {
    String result = this.failover.execute(() -> redis.getClient().get(key));
    if (result == null) {
      return Optional.empty();
    } else {
      return Optional.of(new Value(result));
    }
  }

  @Override
  public void delete(String key) {
    this.failover.execute(() -> redis.getClient().del(key));
  }

  @Override
  public Value putIfAbsent(String key, Value value) {
    this.failover.execute(() -> redis.getClient().setnx(key, value.getAsString()));
    return get(key).get();
  }

  @Override
  public boolean putIfAbsentTtl(String key, Value value, int secondsToExpire) {
    Long result = this.failover.execute(() -> redis.getClient().setnx(key, value.getAsString()));
    if (MUTEX_ACQUIRED.equals(result)) {
      failover.execute(() -> redis.getClient().expire(key, secondsToExpire));
      return true;
    }
    return false;
  }

  @Override
  public void put(String key, Value value) {
    this.failover.execute(() -> redis.getClient().set(key, value.getAsString()));
  }

  @Override
  public void delete(String... keys) {
    this.failover.execute(() -> redis.getClient().del(keys));
  }

  @Override
  public long increase(String key) {
    return this.failover.execute(() -> redis.getClient().incr(key));
  }
}
