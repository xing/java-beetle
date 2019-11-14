package com.xing.beetle.redis;

import com.xing.beetle.dedup.spi.KeyValueStore;
import java.util.Optional;

public class RedisDedupStore implements KeyValueStore<String> {

  private final Redis redis;
  private final BeetleRedisProperties properties;
  private final Failover failover;

  RedisDedupStore(BeetleRedisProperties properties) {
    this.properties = properties;
    this.redis = new Redis(properties);
    this.failover =
        new Failover(
            properties.getRedisFailoverTimeout(), properties.getRedisConfigurationMasterRetries());
  }

  @Override
  public Optional<String> get(String key) {
    return Optional.ofNullable(redis.getClient().get(key));
  }

  @Override
  public boolean putIfAbsent(String key, String value) {
    return 1 == this.failover.execute(() -> redis.getClient().setnx(key, value)).orElse(0L);
  }

  @Override
  public void put(String key, String value) {
    this.failover.execute(() -> redis.getClient().set(key, value));
  }

  @Override
  public void remove(String key) {
    this.failover.execute(() -> redis.getClient().del(key));
  }
}
