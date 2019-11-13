package com.xing.beetle.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "beetle.redis")
public class BeetleRedisProperties {

  private String redisServer;
  private String redisServers;
  private String redisDb;
  private String redisFailoverTimeout;
  private String redisStatusKeyExpiryInterval;
  private String redisFailoverClientHeartbeatInterval;
  private String redisFailoverClientDeadInterval;
  private String redisConfigurationMasterRetries;
  private String redisConfigurationMasterRetryInterval;
  private String redisConfigurationClientTimeout;
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

  public String getRedisFailoverTimeout() {
    return redisFailoverTimeout;
  }

  public void setRedisFailoverTimeout(String redisFailoverTimeout) {
    this.redisFailoverTimeout = redisFailoverTimeout;
  }

  public String getRedisStatusKeyExpiryInterval() {
    return redisStatusKeyExpiryInterval;
  }

  public void setRedisStatusKeyExpiryInterval(String redisStatusKeyExpiryInterval) {
    this.redisStatusKeyExpiryInterval = redisStatusKeyExpiryInterval;
  }

  public String getRedisFailoverClientHeartbeatInterval() {
    return redisFailoverClientHeartbeatInterval;
  }

  public void setRedisFailoverClientHeartbeatInterval(String redisFailoverClientHeartbeatInterval) {
    this.redisFailoverClientHeartbeatInterval = redisFailoverClientHeartbeatInterval;
  }

  public String getRedisFailoverClientDeadInterval() {
    return redisFailoverClientDeadInterval;
  }

  public void setRedisFailoverClientDeadInterval(String redisFailoverClientDeadInterval) {
    this.redisFailoverClientDeadInterval = redisFailoverClientDeadInterval;
  }

  public String getRedisConfigurationMasterRetries() {
    return redisConfigurationMasterRetries;
  }

  public void setRedisConfigurationMasterRetries(String redisConfigurationMasterRetries) {
    this.redisConfigurationMasterRetries = redisConfigurationMasterRetries;
  }

  public String getRedisConfigurationMasterRetryInterval() {
    return redisConfigurationMasterRetryInterval;
  }

  public void setRedisConfigurationMasterRetryInterval(
      String redisConfigurationMasterRetryInterval) {
    this.redisConfigurationMasterRetryInterval = redisConfigurationMasterRetryInterval;
  }

  public String getRedisConfigurationClientTimeout() {
    return redisConfigurationClientTimeout;
  }

  public void setRedisConfigurationClientTimeout(String redisConfigurationClientTimeout) {
    this.redisConfigurationClientTimeout = redisConfigurationClientTimeout;
  }

  public String getRedisConfigurationClientIds() {
    return redisConfigurationClientIds;
  }

  public void setRedisConfigurationClientIds(String redisConfigurationClientIds) {
    this.redisConfigurationClientIds = redisConfigurationClientIds;
  }
}
