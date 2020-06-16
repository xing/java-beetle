package com.xing.beetle.redis;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.dedup.spi.KeyValueStore;
import redis.clients.jedis.params.SetParams;

import java.util.Optional;

/**
 * KeyValueStore based on Redis providing necessary functionality for message status tracking for
 * deduplication. The communication with Redis is with failover logic which uses retries and
 * timeout.
 */
public class RedisDedupStore implements KeyValueStore {

  private final Redis redis;
  private final Failover failover;

  public RedisDedupStore(BeetleAmqpConfiguration beetleAmqpConfiguration) {
    this.redis = new Redis(beetleAmqpConfiguration);
    this.failover = new Failover(beetleAmqpConfiguration.getRedisFailoverTimeoutSeconds(), 1);
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
    String result = "";
    if (secondsToExpire > 0) {
      result =
          this.failover.execute(
              () ->
                  redis
                      .getClient()
                      .set(
                          key,
                          value.getAsString(),
                          SetParams.setParams().nx().ex(secondsToExpire)));
    } else {
      result =
          this.failover.execute(
              () -> redis.getClient().set(key, value.getAsString(), SetParams.setParams().nx()));
    }
    return result != null && result.equals("OK");
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
