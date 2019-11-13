package com.xing.beetle.redis;

public interface RedisConfiguration {

  String getRedisServer();

  String getRedisServers();

  String getRedisDb();

  String getRedisFailoverTimeout();

  String getRedisStatusKeyExpiryInterval();

  String getRedisFailoverClientHeartbeatInterval();

  String getRedisFailoverClientDeadInterval();

  String getRedisConfigurationMasterRetries();

  String getRedisConfigurationMasterRetryInterval();

  String getRedisConfigurationClientTimeout();

  String getRedisConfigurationClientIds();
}
