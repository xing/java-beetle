package com.xing.beetle;

import static com.xing.beetle.Util.currentTimeSeconds;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/** TODO refactor with exception handling/failover */
public class DeduplicationStore {

  private static final Logger log = LoggerFactory.getLogger(DeduplicationStore.class);

  private static final String STATUS = "status";
  private static final String TIMEOUT = "timeout";
  private static final String ATTEMPTS = "attempts";
  private static final String EXCEPTIONS = "exceptions";
  private static final String MUTEX = "mutex";
  private static final String DELAY = "delay";

  private static String key(String messageId, String suffix) {
    return messageId + ":" + suffix;
  }

  private final AtomicReference<JedisPool> poolRef;

  public DeduplicationStore(RedisConfiguration config) {
    log.debug("Connecting to redis master at {}:{}", config.getHostname(), config.getPort());
    JedisPool pool = new JedisPool(new JedisPoolConfig(), config.getHostname(), config.getPort());
    poolRef = new AtomicReference<>(pool);
  }

  /**
   * @param messageId the uuid of the message
   * @param config
   * @return boolean whether to handle the message (true) or not (false)
   */
  public boolean acquireSharedHandlerMutex(String messageId, ConsumerConfiguration config) {
    try (Jedis jedis = safelyGetConnection()) {
      final String timeout =
          Long.toString(
              currentTimeSeconds()
                  + TimeUnit.SECONDS.convert(
                      config.getHandlerTimeout(), config.getHandlerTimeoutUnit()));
      log.debug("Message {}: setting handler timeout to {}", messageId, timeout);
      jedis.set(key(messageId, TIMEOUT), timeout);
      if (jedis.setnx(key(messageId, MUTEX), Long.toString(currentTimeSeconds())) == 0) {
        log.debug("Message {}: removing mutex", messageId);
        jedis.del(key(messageId, MUTEX));
        return false;
      }
      return true;
    }
  }

  public void close() {
    JedisPool pool = poolRef.get();
    log.debug("Closing Jedis connection pool.");
    pool.destroy();
  }

  private String get(String messageId, String field) {
    try (Jedis jedis = safelyGetConnection()) {
      String v = jedis.get(key(messageId, field));
      log.debug("Message {}: Retrieving {} = {}", messageId, field, v);
      return v;
    }
  }

  public long getAttempts(String messageId) {
    String attempts = get(messageId, ATTEMPTS);
    log.debug("Message {}: Attempt count is {}", messageId, attempts);
    return Long.valueOf(attempts == null ? "0" : attempts);
  }

  public HandlerStatus getHandlerStatus(String messageId) {
    try (Jedis jedis = safelyGetConnection()) {
      List<String> statusValues =
          jedis.mget(
              key(messageId, STATUS),
              key(messageId, TIMEOUT),
              key(messageId, ATTEMPTS),
              key(messageId, EXCEPTIONS),
              key(messageId, DELAY));
      return new HandlerStatus(
          statusValues.get(0),
          statusValues.get(1),
          statusValues.get(2),
          statusValues.get(3),
          statusValues.get(4));
    }
  }

  private long increment(String messageId, String key) {
    try (Jedis jedis = safelyGetConnection()) {
      log.debug("Message {}: Incrementing {}", messageId, key);
      return jedis.incr(key(messageId, key));
    }
  }

  public long incrementAttempts(String messageId) {
    return increment(messageId, ATTEMPTS);
  }

  public long incrementExceptions(String messageId) {
    return increment(messageId, EXCEPTIONS);
  }

  public boolean isMessageNew(String messageId, ConsumerConfiguration config) {
    try (Jedis jedis = safelyGetConnection()) {
      if (jedis.msetnx(
              key(messageId, STATUS),
              "incomplete",
              key(messageId, TIMEOUT),
              Long.toString(
                  currentTimeSeconds()
                      + TimeUnit.SECONDS.convert(
                          config.getHandlerTimeout(), config.getHandlerTimeoutUnit())),
              key(messageId, ATTEMPTS),
              "0",
              key(messageId, EXCEPTIONS),
              "0")
          == 1) {
        log.debug(
            "Message {}: Initialized message status, timeout, attempt and exception counts",
            messageId);
        return true;
      }
    }
    log.debug("Message {}: Did not initialize message status, it already exists.", messageId);
    return false;
  }

  public void markMessageCompleted(String messageId) {
    try (Jedis jedis = safelyGetConnection()) {
      jedis.set(key(messageId, STATUS), "complete");
    }
  }

  public void reconnect(RedisConfiguration config) {
    log.debug("Reconnecting to redis master at {}:{}", config.getHostname(), config.getPort());
    final JedisPool jedisPool =
        new JedisPool(new JedisPoolConfig(), config.getHostname(), config.getPort());
    final JedisPool oldPool = poolRef.getAndSet(jedisPool);
    // destroy will allow old instances to be returned to the pool, but new getResource() calls will
    // fail.
    // this means there's a race condition between getting a pool and loaning a resource from it
    // which we need to handle.
    oldPool.destroy();
  }

  public void removeMessageHandlerLock(String messageId, ConsumerConfiguration config) {
    try (Jedis jedis = safelyGetConnection()) {
      jedis.del(key(messageId, MUTEX));
      jedis.set(key(messageId, TIMEOUT), "0");
      jedis.set(
          key(messageId, DELAY),
          Long.toString(
              currentTimeSeconds()
                  + TimeUnit.SECONDS.convert(config.getRetryDelay(), config.getRetryDelayUnit())));
    }
  }

  private Jedis safelyGetConnection() {
    Jedis jedis = null;
    try {
      jedis = poolRef.get().getResource();
    } catch (JedisConnectionException e) {
      // if the pool was already destroyed (because of an ongoing redis master switch), the cause
      // will be
      // an IllegalStateException. Just retry the operation in that case, the next poolRef.get()
      // will return
      // the Jedis connection pool to the new master
      if (e.getCause() instanceof IllegalStateException) {
        jedis = poolRef.get().getResource();
      }
    }
    if (jedis == null) {
      throw new IllegalStateException(
          "No Redis connection available, cannot process message. Check the redis configuration.");
    }
    return jedis;
  }
}
