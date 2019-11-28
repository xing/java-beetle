package com.xing.beetle.redis;

import com.xing.beetle.dedup.spi.KeyValueStore;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            this.properties.getRedisFailoverTimeout(),
            this.properties.getRedisConfigurationMasterRetries());
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
    return 1 == this.failover.execute(() -> redis.getClient().msetnx(arguments));
  }

  @Override
  public List<Value> getAll(List<String> keys) {
    List<String> result =
        this.failover.execute(() -> redis.getClient().mget(keys.stream().toArray(String[]::new)));
    return result.stream().map(Value::new).collect(Collectors.toList());
  }

  @Override
  public long increase(String key) {
    return this.failover.execute(() -> redis.getClient().incr(key));
  }

  @Override
  public long decrease(String key) {
    return this.failover.execute(() -> redis.getClient().decr(key));
  }

  @Override
  public boolean exists(String key) {
    return this.failover.execute(() -> redis.getClient().exists(key));
  }

  private String[] createKeyValueArgs(Map<String, Value> keyValues) {
    return keyValues.entrySet().stream()
        .map(e -> List.of(e.getKey(), e.getValue().getAsString()))
        .flatMap(Collection::stream)
        .toArray(String[]::new);
  }
}
