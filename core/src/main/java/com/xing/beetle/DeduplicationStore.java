package com.xing.beetle;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 */
public class DeduplicationStore {

    private final JedisPool pool;

    public DeduplicationStore() {
        // TODO use system configuration message and write current master to file
        pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1", 6379);
    }



}
