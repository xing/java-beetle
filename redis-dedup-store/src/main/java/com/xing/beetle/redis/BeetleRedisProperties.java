package com.xing.beetle.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "beetle.redis")
public class BeetleRedisProperties {

  private String redisDb;
  private int redisFailoverTimeout;
  private int redisConfigurationMasterRetries;
  private int redisConfigurationMasterRetryInterval;
  private int redisConfigurationClientTimeout;
  private String redisConfigurationClientIds;

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
}
