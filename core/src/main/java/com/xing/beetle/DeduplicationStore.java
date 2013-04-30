package com.xing.beetle;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.List;

/**
 * TODO refactor with exception handling/failover
 */
public class DeduplicationStore {

    public static final String STATUS = "status";
    public static final String TIMEOUT = "timeout";
    public static final String ATTEMPTS = "attempts";
    public static final String EXCEPTIONS = "exceptions";
    public static final String MUTEX = "mutex";
    public static final String DELAY = "delay";
    private final JedisPool pool;

    public DeduplicationStore(RedisConfiguration config) {
        // TODO use system configuration message and write current master to file
        pool = new JedisPool(new JedisPoolConfig(), config.getHostname(), config.getPort());
    }

    public boolean isMessageNew(String messageId) {
        final Jedis jedis = pool.getResource();

        try {
            if (jedis.msetnx(
                key(messageId, STATUS), "incomplete",
                key(messageId, TIMEOUT), Long.toString((System.currentTimeMillis() / 1000L) + 600), // TODO get from handler config
                key(messageId, ATTEMPTS), "0",
                key(messageId, EXCEPTIONS), "0"
            ) == 1) {
                return true;
            }
        } finally {
            pool.returnResource(jedis);
        }
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
        final Jedis jedis = pool.getResource();
        try {
            jedis.set(key(messageId, STATUS), "complete");
        } finally {
            pool.returnResource(jedis);
        }
    }

    public void removeMessageHandlerLock(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            jedis.del(key(messageId, MUTEX));
            jedis.set(key(messageId, TIMEOUT), "0");
            jedis.set(key(messageId, DELAY), Long.toString((System.currentTimeMillis() / 1000L) + 10)); // TODO get from handler config
        } finally {
            pool.returnResource(jedis);
        }
    }

    public HandlerStatus getHandlerStatus(String messageId) {
        final Jedis jedis = pool.getResource();
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
     * @param messageId
     * @return boolean whether to handle the message (true) or not (false)
     */
    public boolean acquireSharedHandlerMutex(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            jedis.set(key(messageId, TIMEOUT), Long.toString((System.currentTimeMillis() / 1000L) + 600));
            if (jedis.setnx(key(messageId, MUTEX), Long.toString(System.currentTimeMillis() / 1000L)) == 0) {
                jedis.del(key(messageId, MUTEX));
                return false;
            }
            return true;
        } finally {
            pool.returnResource(jedis);
        }
    }

    public void close() {
        pool.destroy();
    }

    private String get(String messageId, String field) {
        final Jedis jedis = pool.getResource();
        try {
            return jedis.get(key(messageId, field));
        } finally {
            pool.returnResource(jedis);
        }
    }

    private long increment(String messageId, String key) {
        final Jedis jedis = pool.getResource();
        try {
            return jedis.incr(key(messageId, key));
        } finally {
            pool.returnResource(jedis);
        }
    }
}
