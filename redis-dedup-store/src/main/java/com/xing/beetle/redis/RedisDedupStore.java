package com.xing.beetle.redis;

import com.xing.beetle.dedup.spi.KeyValueStore;
import java.util.Optional;
import redis.clients.jedis.Jedis;

public class RedisDedupStore implements KeyValueStore<String> {

  private Jedis jedis;

  public RedisDedupStore(RedisConfiguration redisConfiguration) {
    this.jedis = new Jedis(redisConfiguration.getRedisServer());
  }

  @Override
  public Optional<String> get(String key) {
    return Optional.ofNullable(jedis.get(key));
  }

  @Override
  public boolean putIfAbsent(String key, String value) {
    return 1 == jedis.setnx(key, value);
  }

  @Override
  public void put(String key, String value) {
    jedis.set(key, value);
  }

  @Override
  public void remove(String key) {
    jedis.del(key);
  }
}
