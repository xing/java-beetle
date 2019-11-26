package com.xing.beetle.redis;

import com.xing.beetle.dedup.spi.KeyValueStore;

import java.util.*;
import java.util.stream.Collectors;

public class RedisDedupStore implements KeyValueStore {

  private static final Long MUTEX_ACQUIRED = 1L;

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
  public Optional<Value> get(String key) {
    return this.failover.execute(() -> redis.getClient().get(key)).map(Value::new);
  }

  @Override
  public Value putIfAbsent(String key, Value value) {
    this.failover.execute(() -> redis.getClient().setnx(key, value.getAsString()));
    return get(key).get();
  }

  @Override
  public boolean putIfAbsentTtl(String key, Value value, int secondsToExpire) {
    boolean acquired =
        this.failover
            .execute(() -> redis.getClient().setnx(key, value.getAsString()))
            .filter(MUTEX_ACQUIRED::equals)
            .isPresent();
    if (acquired) {
      failover.execute(() -> redis.getClient().expire(key, secondsToExpire));
    }
    return acquired;
  }

  @Override
  public void put(String key, Value value) {
    this.failover.execute(() -> redis.getClient().set(key, value.getAsString()));
  }

  @Override
  public void remove(String... keys) {
    this.failover.execute(() -> redis.getClient().del(keys));
  }

  @Override
  public void putAll(Map<String, Value> keyValues) {
    String[] arguments = createKeyValueArgs(keyValues);
    this.failover.execute(() -> redis.getClient().mset(arguments));
  }

  @Override
  public boolean putAllIfAbsent(Map<String, Value> keyValues) {
    String[] arguments = createKeyValueArgs(keyValues);
    return 1 == this.failover.execute(() -> redis.getClient().msetnx(arguments)).orElse(0L);
  }

  @Override
  public List<Value> getAll(List<String> keys) {
    return this.failover.execute(() -> redis.getClient().mget(keys.stream().toArray(String[]::new)))
        .orElse(Collections.emptyList()).stream()
        .map(Value::new)
        .collect(Collectors.toList());
  }

  @Override
  public long increase(String key) {
    return this.failover.execute(() -> redis.getClient().incr(key)).orElse(1L);
  }

  @Override
  public long decrease(String key) {
    return this.failover.execute(() -> redis.getClient().decr(key)).orElse(-1L);
  }

  @Override
  public boolean exists(String key) {
    return this.failover.execute(() -> redis.getClient().exists(key)).orElse(false);
  }

  private String[] createKeyValueArgs(Map<String, Value> keyValues) {
    return keyValues.entrySet().stream()
        .map(e -> List.of(e.getKey(), e.getValue().getAsString()))
        .flatMap(Collection::stream)
        .toArray(String[]::new);
  }
}
