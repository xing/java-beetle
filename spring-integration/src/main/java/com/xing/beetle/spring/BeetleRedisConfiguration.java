package com.xing.beetle.spring;

import com.xing.beetle.redis.RedisConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "beetle.redis")
public class BeetleRedisConfiguration implements RedisConfiguration {

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

  @Override
  public String getRedisServer() {
    return redisServer;
  }

  public void setRedisServer(String redisServer) {
    this.redisServer = redisServer;
  }

  @Override
  public String getRedisServers() {
    return redisServers;
  }

  public void setRedisServers(String redisServers) {
    this.redisServers = redisServers;
  }

  @Override
  public String getRedisDb() {
    return redisDb;
  }

  public void setRedisDb(String redisDb) {
    this.redisDb = redisDb;
  }

  @Override
  public String getRedisFailoverTimeout() {
    return redisFailoverTimeout;
  }

  public void setRedisFailoverTimeout(String redisFailoverTimeout) {
    this.redisFailoverTimeout = redisFailoverTimeout;
  }

  @Override
  public String getRedisStatusKeyExpiryInterval() {
    return redisStatusKeyExpiryInterval;
  }

  public void setRedisStatusKeyExpiryInterval(String redisStatusKeyExpiryInterval) {
    this.redisStatusKeyExpiryInterval = redisStatusKeyExpiryInterval;
  }

  @Override
  public String getRedisFailoverClientHeartbeatInterval() {
    return redisFailoverClientHeartbeatInterval;
  }

  public void setRedisFailoverClientHeartbeatInterval(String redisFailoverClientHeartbeatInterval) {
    this.redisFailoverClientHeartbeatInterval = redisFailoverClientHeartbeatInterval;
  }

  @Override
  public String getRedisFailoverClientDeadInterval() {
    return redisFailoverClientDeadInterval;
  }

  public void setRedisFailoverClientDeadInterval(String redisFailoverClientDeadInterval) {
    this.redisFailoverClientDeadInterval = redisFailoverClientDeadInterval;
  }

  @Override
  public String getRedisConfigurationMasterRetries() {
    return redisConfigurationMasterRetries;
  }

  public void setRedisConfigurationMasterRetries(String redisConfigurationMasterRetries) {
    this.redisConfigurationMasterRetries = redisConfigurationMasterRetries;
  }

  @Override
  public String getRedisConfigurationMasterRetryInterval() {
    return redisConfigurationMasterRetryInterval;
  }

  public void setRedisConfigurationMasterRetryInterval(
      String redisConfigurationMasterRetryInterval) {
    this.redisConfigurationMasterRetryInterval = redisConfigurationMasterRetryInterval;
  }

  @Override
  public String getRedisConfigurationClientTimeout() {
    return redisConfigurationClientTimeout;
  }

  public void setRedisConfigurationClientTimeout(String redisConfigurationClientTimeout) {
    this.redisConfigurationClientTimeout = redisConfigurationClientTimeout;
  }

  @Override
  public String getRedisConfigurationClientIds() {
    return redisConfigurationClientIds;
  }

  public void setRedisConfigurationClientIds(String redisConfigurationClientIds) {
    this.redisConfigurationClientIds = redisConfigurationClientIds;
  }
}
