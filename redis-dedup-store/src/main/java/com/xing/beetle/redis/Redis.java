package com.xing.beetle.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Encapsulates a Redis client (Jedis) which can switch connections when update() is called. */
public class Redis {
  private static Logger logger = LoggerFactory.getLogger(Redis.class);
  private BeetleRedisProperties config;
  private String activeMaster = "";
  private Jedis client;
  private long lastMasterChanged;
  private ReentrantLock lock = new ReentrantLock();
  private Condition connected = lock.newCondition();

  Redis(BeetleRedisProperties config) {
    this.config = config;
    this.update();
  }

  public String getSystem() {
    return config.getSystemName();
  }

  public BeetleRedisProperties getConfig() {
    return config;
  }

  public String getActiveMaster() {
    return activeMaster;
  }

  Jedis getClient() {
    lock.lock();
    try {
      while (client == null) {
        connected.await();
      }
      connected.signalAll();
      return client;
    } catch (InterruptedException e) {
      throw new DeduplicationException("Client is not connected!", e);
    } finally {
      lock.unlock();
    }
  }

  public long getLastMasterChanged() {
    return lastMasterChanged;
  }

  /** Reads the Redis server configuration and updates the client connection accordingly. */
  private void update() {
    String serverAddress;
    if (Files.exists(Paths.get(config.getRedisServer()))) {
      File file = new File(config.getRedisServer());
      lastMasterChanged = file.lastModified();
      serverAddress = extractRedisMaster(file);
    } else {
      logger.debug(
          "Server configuration {} is not a file. Using {} as a server address.",
          config.getRedisServer(),
          config.getRedisServer());
      serverAddress = config.getRedisServer();
    }
    if (activeMaster.equals(serverAddress)) {
      logger.debug("Master unchanged");
    } else {
      activeMaster = serverAddress;
      if (activeMaster.isEmpty() && client != null) {
        client.disconnect();
      } else {
        if (client != null) {
          client.disconnect();
        }
        client = new Jedis(HostAndPort.parseString(activeMaster));
        client.connect();
      }
    }
  }
  /** Reads the Redis server address from the configuration file. */
  private String extractRedisMaster(File file) {
    try {
      List<String> lines = Files.readAllLines(file.toPath());
      for (String line : lines) {
        String[] parts = line.split("/", 2);
        if (parts.length == 1) {
          return parts[0];
        } else if (parts.length == 2) {
          if (parts[0].equals(config.getSystemName())) {
            return parts[1];
          }
        }
      }
    } catch (IOException e) {
      logger.error("Unable to read the redis configuration", e);
    }
    return "";
  }
}
