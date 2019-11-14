package com.xing.beetle.redis.failover;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * RedisServerInfo contans a list slice of RedisShim objects and a lookup index on server strings
 * (host:port format).
 */
public class RedisServerInfo {

  private static Logger logger = LoggerFactory.getLogger(RedisServerInfo.class);
  private List<RedisShim> instances;
  private Map<RedisShim.Role, List<RedisShim>> serverInfo;

  boolean include(RedisShim redisShim) {
    return instances.stream().anyMatch(r -> r.getServer().equals(redisShim.getServer()));
  }

  List<String> servers() {
    return instances.stream().map(RedisShim::getServer).collect(Collectors.toList());
  }

  static RedisServerInfo newRedisServerInfo(String servers) {
    RedisServerInfo redisServerInfo = new RedisServerInfo();
    if (servers != null && !servers.isEmpty()) {
      String[] serverList = Pattern.compile(" *, *").split(servers);
      redisServerInfo.instances =
          Arrays.stream(serverList)
              .map(
                  s -> {
                    logger.info("adding redis server: {}", s);
                    return RedisShim.newRedisShim(s);
                  })
              .collect(Collectors.toList());
    }
    redisServerInfo.reset();
    return redisServerInfo;
  }

  private void reset() {
    serverInfo = new HashMap<>();
    for (RedisShim.Role role : RedisShim.Role.values()) {
      serverInfo.put(role, new ArrayList<>());
    }
  }

  int numServers() {
    return instances.size();
  }

  void refresh() {
    logger.info("refreshing server info");
    reset();
    instances.forEach(
        redisShim -> {
          logger.info("determined {} to be a {}", redisShim.getServer(), redisShim.role());
          serverInfo.get(redisShim.role()).add(redisShim);
        });
  }

  Optional<Jedis> find(String server) {
    return instances.stream()
        .filter(redisShim -> redisShim.getServer().equals(server))
        .map(RedisShim::getRedis)
        .findFirst();
  }

  List<RedisShim> masters() {
    return serverInfo.get(RedisShim.Role.MASTER);
  }

  List<RedisShim> slaves() {
    return serverInfo.get(RedisShim.Role.SLAVE);
  }

  List<RedisShim> unknowns() {
    return serverInfo.get(RedisShim.Role.UNKNOWN);
  }

  List<RedisShim> slavesOf(RedisShim master) {
    return slaves().stream()
        .filter(redisShim -> redisShim.isSlaveOf(master.getHost(), master.getPort()))
        .collect(Collectors.toList());
  }

  RedisShim autoDetectMaster() {
    if (masterAndSlavesReachable()) {
      masters().get(0);
    }
    return null;
  }

  boolean masterAndSlavesReachable() {
    return masters().size() == 1 && slaves().size() == instances.size() - 1;
  }
}
