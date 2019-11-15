package com.xing.beetle.redis;

import com.xing.beetle.dedup.spi.KeyValueStore;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import redis.clients.jedis.Jedis;

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
  public void remove(String... keys) {
    this.failover.execute(() -> redis.getClient().del(keys));
  }

  @Override
  public void putAll(Map<String, String> keyValues) {
    String[] arguments = createKeyValueArgs(keyValues);
    this.failover.execute(() -> redis.getClient().mset(arguments));
  }

  @Override
  public boolean putAllIfAbsent(Map<String, String> keyValues) {
    String[] arguments = createKeyValueArgs(keyValues);
    return 1 == this.failover.execute(() -> redis.getClient().msetnx(arguments)).orElse(0L);
  }

  @Override
  public List<String> getAll(List<String> keys) {
    return this.failover
        .execute(() -> redis.getClient().mget(keys.stream().toArray(String[]::new)))
        .orElse(List.of());
  }

  @Override
  public String increase(String key) {
    return this.failover.execute(() -> String.valueOf(redis.getClient().incr(key))).orElse("");
  }

  @Override
  public String decrease(String key) {
    return this.failover.execute(() -> String.valueOf(redis.getClient().decr(key))).orElse("");
  }

  @Override
  public boolean exists(String key) {
    return this.failover.execute(() -> redis.getClient().exists(key)).orElse(false);
  }

  private String[] createKeyValueArgs(Map<String, String> keyValues) {
    return keyValues.entrySet().stream()
        .map(e -> List.of(e.getKey(), e.getValue()))
        .flatMap(Collection::stream)
        .toArray(String[]::new);
  }

  public static void main(String[] args) {
    Jedis jedis = new Jedis();
    String mset = jedis.mset("key1", "value1", "key2", "value2");
    System.out.println(mset);
    System.out.println(jedis.get("key1"));
    System.out.println(jedis.get("key2"));
    String rr = jedis.mset("key1", "value2", "key3", "value2");
    System.out.println(jedis.get("key1"));
    System.out.println("ok");
  }
}
