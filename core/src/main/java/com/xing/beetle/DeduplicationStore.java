package com.xing.beetle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.xing.beetle.Util.currentTimeSeconds;

/**
 * TODO refactor with exception handling/failover
 */
public class DeduplicationStore {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationStore.class);

    private static final String STATUS = "status";
    private static final String TIMEOUT = "timeout";
    private static final String ATTEMPTS = "attempts";
    private static final String EXCEPTIONS = "exceptions";
    private static final String MUTEX = "mutex";
    private static final String DELAY = "delay";
    private final AtomicReference<JedisPool> poolRef;

    public DeduplicationStore(RedisConfiguration config) {
        log.debug("Connecting to redis master at {}:{}", config.getHostname(), config.getPort());
        JedisPool pool = new JedisPool(new JedisPoolConfig(), config.getHostname(), config.getPort());
        poolRef = new AtomicReference<>(pool);
    }

    public void reconnect(RedisConfiguration config) {
        log.debug("Reconnecting to redis master at {}:{}", config.getHostname(), config.getPort());
        final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), config.getHostname(), config.getPort());
        final JedisPool oldPool = poolRef.getAndSet(jedisPool);
        // destroy will allow old instances to be returned to the pool, but new getResource() calls will fail.
        // this means there's a race condition between getting a pool and loaning a resource from it which we need to handle.
        oldPool.destroy();
    }

    public boolean isMessageNew(String messageId, ConsumerConfiguration config) {
        final Pair<Jedis, JedisPool> connAndPool = safelyGetConnection();
        final Jedis jedis = connAndPool.getLeft();
        final JedisPool pool = connAndPool.getRight();

        try {
            if (jedis.msetnx(
                key(messageId, STATUS), "incomplete",
                key(messageId, TIMEOUT),
                    Long.toString(currentTimeSeconds() + TimeUnit.SECONDS.convert(config.getHandlerTimeout(), config.getHandlerTimeoutUnit())),
                key(messageId, ATTEMPTS), "0",
                key(messageId, EXCEPTIONS), "0"
            ) == 1) {
                log.debug("Message {}: Initialized message status, timeout, attempt and exception counts", messageId);
                return true;
            }
        } finally {
            pool.returnResource(jedis);
        }
        log.debug("Message {}: Did not initialize message status, it already exists.", messageId);
        return false;
    }

    private static String key(String messageId, String suffix) {
        return messageId + ":" + suffix;
    }

    public long incrementAttempts(String messageId) {
        return increment(messageId, ATTEMPTS);
    }

    public long incrementExceptions(String messageId) {
        return increment(messageId, EXCEPTIONS);
    }

    public void markMessageCompleted(String messageId) {
        final Pair<Jedis, JedisPool> connAndPool = safelyGetConnection();
        final Jedis jedis = connAndPool.getLeft();
        final JedisPool pool = connAndPool.getRight();
        try {
            jedis.set(key(messageId, STATUS), "complete");
        } finally {
            pool.returnResource(jedis);
        }
    }

    public void removeMessageHandlerLock(String messageId, ConsumerConfiguration config) {
        final Pair<Jedis, JedisPool> connAndPool = safelyGetConnection();
        final Jedis jedis = connAndPool.getLeft();
        final JedisPool pool = connAndPool.getRight();

        try {
            jedis.del(key(messageId, MUTEX));
            jedis.set(key(messageId, TIMEOUT), "0");
            jedis.set(key(messageId, DELAY), Long.toString(currentTimeSeconds() + TimeUnit.SECONDS.convert(config.getRetryDelay(), config.getRetryDelayUnit())));
        } finally {
            pool.returnResource(jedis);
        }
    }

    public HandlerStatus getHandlerStatus(String messageId) {
        final Pair<Jedis, JedisPool> connAndPool = safelyGetConnection();
        final Jedis jedis = connAndPool.getLeft();
        final JedisPool pool = connAndPool.getRight();

        try {
            final List<String> statusValues = jedis.mget(
                key(messageId, STATUS),
                key(messageId, TIMEOUT),
                key(messageId, ATTEMPTS),
                key(messageId, EXCEPTIONS),
                key(messageId, DELAY));
            return new HandlerStatus(statusValues.get(0), statusValues.get(1), statusValues.get(2), statusValues.get(3), statusValues.get(4));

        } finally {
            pool.returnResource(jedis);
        }
    }

    /**
     *
     *
     * @param messageId the uuid of the message
     * @param config
     * @return boolean whether to handle the message (true) or not (false)
     */
    public boolean acquireSharedHandlerMutex(String messageId, ConsumerConfiguration config) {
        final Pair<Jedis, JedisPool> connAndPool = safelyGetConnection();
        final Jedis jedis = connAndPool.getLeft();
        final JedisPool pool = connAndPool.getRight();
        try {
            final String timeout = Long.toString(currentTimeSeconds() + TimeUnit.SECONDS.convert(config.getHandlerTimeout(), config.getHandlerTimeoutUnit()));
            log.debug("Message {}: setting handler timeout to {}", messageId, timeout);
            jedis.set(key(messageId, TIMEOUT), timeout);
            if (jedis.setnx(key(messageId, MUTEX), Long.toString(currentTimeSeconds())) == 0) {
                log.debug("Message {}: removing mutex", messageId);
                jedis.del(key(messageId, MUTEX));
                return false;
            }
            return true;
        } finally {
            pool.returnResource(jedis);
        }
    }

    public void close() {
        JedisPool pool = poolRef.get();
        log.debug("Closing Jedis connection pool.");
        pool.destroy();
    }

    private String get(String messageId, String field) {
        final Pair<Jedis, JedisPool> connAndPool = safelyGetConnection();
        final Jedis jedis = connAndPool.getLeft();
        final JedisPool pool = connAndPool.getRight();
        try {
            final String v = jedis.get(key(messageId, field));
            log.debug("Message {}: Retrieving {} = {}", messageId, field, v);
            return v;
        } finally {
            pool.returnResource(jedis);
        }
    }

    private long increment(String messageId, String key) {
        final Pair<Jedis, JedisPool> connAndPool = safelyGetConnection();
        final Jedis jedis = connAndPool.getLeft();
        final JedisPool pool = connAndPool.getRight();
        try {
            log.debug("Message {}: Incrementing {}", messageId, key);
            return jedis.incr(key(messageId, key));
        } finally {
            pool.returnResource(jedis);
        }
    }

    public long getAttempts(String messageId) {
        final String attempts = get(messageId, ATTEMPTS);
        log.debug("Message {}: Attempt count is {}", messageId, attempts);
        return Long.valueOf(attempts == null ? "0" : attempts);
    }

    private Pair<Jedis,JedisPool> safelyGetConnection() {
        Jedis jedis = null;
        JedisPool pool = null;
        try {
            pool = poolRef.get();
            jedis = pool.getResource();
        } catch (JedisConnectionException e) {
            // if the pool was already destroyed (because of an ongoing redis master switch), the cause will be
            // an IllegalStateException. Just retry the operation in that case, the next poolRef.get() will return
            // the Jedis connection pool to the new master
            if (e.getCause() instanceof IllegalStateException) {
                pool = poolRef.get();
                jedis = pool.getResource();
            }
        }
        if (jedis == null) {
            throw new IllegalStateException("No Redis connection available, cannot process message. Check the redis configuration.");
        }

        return Pair.createPair(jedis, pool);
    }
}
