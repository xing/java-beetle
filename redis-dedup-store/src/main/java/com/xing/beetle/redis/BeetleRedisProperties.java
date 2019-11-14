package com.xing.beetle.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "beetle.redis")
public class BeetleRedisProperties {

  private String systemName;
  private String redisServer;
  private String redisServers;
  private String redisDb;
  private int redisFailoverTimeout;
  private int redisStatusKeyExpiryInterval;
  private int redisFailoverClientHeartbeatInterval;
  private int redisFailoverClientDeadInterval;
  private int redisConfigurationMasterRetries;
  private int redisConfigurationMasterRetryInterval;
  private int redisConfigurationClientTimeout;
  private String redisConfigurationClientIds;

  public String getRedisServer() {
    return redisServer;
  }

  public void setRedisServer(String redisServer) {
    this.redisServer = redisServer;
  }

  public String getRedisServers() {
    return redisServers;
  }

  public void setRedisServers(String redisServers) {
    this.redisServers = redisServers;
  }

  public String getRedisDb() {
    return redisDb;
  }

  public void setRedisDb(String redisDb) {
    this.redisDb = redisDb;
  }

  public int getRedisFailoverTimeout() {
    return redisFailoverTimeout;
  }

  public void setRedisFailoverTimeout(int redisFailoverTimeout) {
    this.redisFailoverTimeout = redisFailoverTimeout;
  }

  public int getRedisStatusKeyExpiryInterval() {
    return redisStatusKeyExpiryInterval;
  }

  public void setRedisStatusKeyExpiryInterval(int redisStatusKeyExpiryInterval) {
    this.redisStatusKeyExpiryInterval = redisStatusKeyExpiryInterval;
  }

  public int getRedisFailoverClientHeartbeatInterval() {
    return redisFailoverClientHeartbeatInterval;
  }

  public void setRedisFailoverClientHeartbeatInterval(int redisFailoverClientHeartbeatInterval) {
    this.redisFailoverClientHeartbeatInterval = redisFailoverClientHeartbeatInterval;
  }

  public int getRedisFailoverClientDeadInterval() {
    return redisFailoverClientDeadInterval;
  }

  public void setRedisFailoverClientDeadInterval(int redisFailoverClientDeadInterval) {
    this.redisFailoverClientDeadInterval = redisFailoverClientDeadInterval;
  }

  public int getRedisConfigurationMasterRetries() {
    return redisConfigurationMasterRetries;
  }

  public void setRedisConfigurationMasterRetries(int redisConfigurationMasterRetries) {
    this.redisConfigurationMasterRetries = redisConfigurationMasterRetries;
  }

  public int getRedisConfigurationMasterRetryInterval() {
    return redisConfigurationMasterRetryInterval;
  }

  public void setRedisConfigurationMasterRetryInterval(int redisConfigurationMasterRetryInterval) {
    this.redisConfigurationMasterRetryInterval = redisConfigurationMasterRetryInterval;
  }

  public int getRedisConfigurationClientTimeout() {
    return redisConfigurationClientTimeout;
  }

  public void setRedisConfigurationClientTimeout(int redisConfigurationClientTimeout) {
    this.redisConfigurationClientTimeout = redisConfigurationClientTimeout;
  }

  public String getRedisConfigurationClientIds() {
    return redisConfigurationClientIds;
  }

  public void setRedisConfigurationClientIds(String redisConfigurationClientIds) {
    this.redisConfigurationClientIds = redisConfigurationClientIds;
  }

  public String getSystemName() {
    return systemName;
  }

  public void setSystemName(String systemName) {
    this.systemName = systemName;
  }
}
