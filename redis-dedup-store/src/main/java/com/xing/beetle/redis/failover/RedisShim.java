package com.xing.beetle.redis.failover;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * RedisShim contains info about server name and port and a pointer to the underlying redis client.
 */
class RedisShim {
  private static Logger logger = LoggerFactory.getLogger(RedisShim.class);
  private Jedis redis;
  private String server;
  private String host;
  private int port;

  enum Role {
    MASTER,
    SLAVE,
    UNKNOWN
  };

  static RedisShim newRedisShim(String server) {
    RedisShim shim = new RedisShim();
    shim.server = server;
    String[] parts = server.split(":");
    shim.host = parts[0];
    shim.port = Integer.parseInt(parts[1]);
    shim.redis = new Jedis(shim.host, shim.port);
    return shim;
  }

  public String getServer() {
    return server;
  }

  public Jedis getRedis() {
    return redis;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  Map<String, String> info() {
    long start = System.currentTimeMillis();
    String replication = redis.info("Replication");
    long end = System.currentTimeMillis();
    logger.info("obtaining redis info from {} took {} ms", server, (end - start));
    Pattern regex = Pattern.compile("[^:]+:.*");
    String[] strings = replication.split("\n");
    Map<String, String> result = new HashMap<>();
    for (String string : strings) {
      List<MatchResult> results = regex.matcher(string).results().collect(Collectors.toList());
      if (results.size() == 1) {
        String[] split = results.get(0).group().split(":");
        result.put(split[0], split[1]);
      }
    }
    return result;
  }

  void dumpMap(Map<String, String> map) {
    System.out.println("MAP[");
    map.forEach((k, v) -> System.out.printf("%s : %s \n", k, v));
    System.out.println("]MAP");
  }

  Role role() {
    Map<String, String> info = info();
    return Role.valueOf(info.getOrDefault("role", Role.UNKNOWN.name()));
  }

  boolean isMaster() {
    return role() == Role.MASTER;
  }

  boolean isSlave() {
    return role() == Role.SLAVE;
  }

  boolean isAvailable() {
    long start = System.currentTimeMillis();
    String result = redis.ping();
    long end = System.currentTimeMillis();
    if (!result.equals("PONG")) {
      logger.error("pinging server {} failed: {}", server, result);
    }
    logger.info("pinging server {} took {} ms", server, (end - start));

    return true;
  }

  public static void main(String[] args) {
    RedisShim redisShim = newRedisShim("localhost:6379");
    Map<String, String> info = redisShim.info();
    redisShim.dumpMap(info);
    redisShim.isAvailable();
    redisShim.makeMaster();
  }

  // makeMaster sends SLAVEOF no one to the redis server.
  void makeMaster() {
    long start = System.currentTimeMillis();
    String result = redis.slaveofNoOne();
    long end = System.currentTimeMillis();
    if (!result.equals("OK")) {
      logger.error("could not make {} a slave of no one: {}", server, result);
    } else {
      logger.info("making {} a slave of no one took {} ms", server, (end - start));
    }
  }

  // isSlaveOf checks whether the redis server is a slave of some other server.
  boolean isSlaveOf(String host, int port) {
    Map<String, String> info = info();
    return role() == Role.SLAVE
        && host.equals(info.get("master_host"))
        && String.valueOf(port).equals(info.get("master_port"));
  }

  // redisMakeSlave makes the redis server slave of some other server.
  void redisMakeSlave(String host, int port) {
    long start = System.currentTimeMillis();
    String result = redis.slaveof(host, port);
    long end = System.currentTimeMillis();
    if (!result.equals("OK")) {
      logger.error("could not make {} a slave of {}:{}: {}", server, host, port, result);
    } else {
      logger.info("making {} a slave of {}:{}took {} ms", server, host, port, (end - start));
    }
  }
}
