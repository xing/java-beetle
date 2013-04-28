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

    public DeduplicationStore(String hostname, int port) {
        // TODO use system configuration message and write current master to file
        pool = new JedisPool(new JedisPoolConfig(), hostname, port);
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

    public void incrementAttempts(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            jedis.incr(key(messageId, ATTEMPTS));
        } finally {
            pool.returnResource(jedis);
        }
    }

    public void markMessageCompleted(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            jedis.set(key(messageId, STATUS), "complete");
        } finally {
            pool.returnResource(jedis);
        }
    }

    public long incrementExceptions(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            return jedis.incr(key(messageId, EXCEPTIONS));
        } finally {
            pool.returnResource(jedis);
        }
    }

    public long getAttemptsCount(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            return Long.valueOf(jedis.get(key(messageId, ATTEMPTS)));
        } finally {
            pool.returnResource(jedis);
        }
    }

    public long getExceptionsCount(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            return Long.valueOf(jedis.get(key(messageId, EXCEPTIONS)));
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

    public HashMap<String, String> getHandlerStatus(String messageId) {
        final Jedis jedis = pool.getResource();
        try {
            final List<String> statusValues = jedis.mget(
                key(messageId, STATUS),
                key(messageId, TIMEOUT),
                key(messageId, ATTEMPTS),
                key(messageId, EXCEPTIONS));
            final HashMap<String, String> handlerStatus = new HashMap<>();
            handlerStatus.put(STATUS, statusValues.get(0));
            handlerStatus.put(TIMEOUT, statusValues.get(1));
            handlerStatus.put(ATTEMPTS, statusValues.get(2));
            handlerStatus.put(EXCEPTIONS, statusValues.get(3));
            return handlerStatus;
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
}
