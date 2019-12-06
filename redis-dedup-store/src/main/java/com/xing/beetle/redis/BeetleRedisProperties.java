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
  private int redisConfigurationMasterRetries;
  private int redisConfigurationMasterRetryInterval;
  private int redisConfigurationClientTimeout;
  private String redisConfigurationClientIds;

  String getRedisServer() {
    return redisServer;
  }

  void setRedisServer(String redisServer) {
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

  int getRedisFailoverTimeout() {
    return redisFailoverTimeout;
  }

  void setRedisFailoverTimeout(int redisFailoverTimeout) {
    this.redisFailoverTimeout = redisFailoverTimeout;
  }

  public int getRedisStatusKeyExpiryInterval() {
    return redisStatusKeyExpiryInterval;
  }

  public void setRedisStatusKeyExpiryInterval(int redisStatusKeyExpiryInterval) {
    this.redisStatusKeyExpiryInterval = redisStatusKeyExpiryInterval;
  }

  int getRedisConfigurationMasterRetries() {
    return redisConfigurationMasterRetries;
  }

  void setRedisConfigurationMasterRetries(int redisConfigurationMasterRetries) {
    this.redisConfigurationMasterRetries = redisConfigurationMasterRetries;
  }

  int getRedisConfigurationMasterRetryInterval() {
    return redisConfigurationMasterRetryInterval;
  }

  void setRedisConfigurationMasterRetryInterval(int redisConfigurationMasterRetryInterval) {
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

  String getSystemName() {
    return systemName;
  }

  void setSystemName(String systemName) {
    this.systemName = systemName;
  }
}
