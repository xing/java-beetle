package com.xing.beetle.amqp;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Beetle Amqp configuration defined by Environment variables. */
@Configuration
@Profile("!test")
public class BeetleAmqpConfiguration {

  private String systemName;
  private String beetlePolicyExchangeName = "beetle-policies";
  private String beetlePolicyUpdatesQueueName = "beetle-policy-updates";
  private String beetlePolicyUpdatesRoutingKey = "beetle.policy.update";

  private String beetleRedisServer;
  private String beetleServers;
  private String beetleAdditionalSubscriptionServers;
  private int beetleRedisStatusKeyExpiryInterval;

  private int redisFailoverTimeout = 180;

  public int getRedisFailoverTimeout() {
    return redisFailoverTimeout;
  }

  void setRedisFailoverTimeout(int redisFailoverTimeout) {
    this.redisFailoverTimeout = redisFailoverTimeout;
  }

  private boolean deadLetteringEnabled = false;
  private boolean lazyQueuesEnabled = false;
  private int deadLetteringMsgTtl = 1000; // 1 second
  private String user = "guest";
  private String password = "guest";

  private int handlerTimeout = 600;
  private int maxHandlerExecutionAttempts = 1;
  private int handlerExecutionAttemptsDelay = 10;
  private int exceptionLimit = 0;

  public BeetleAmqpConfiguration() {
    beetleRedisServer = System.getenv("BEETLE_REDIS_SERVER");
    if (beetleRedisServer == null) {
      beetleRedisServer = "/virtual/beetle/redis-master-rcc";
    }

    beetleServers = System.getenv("BEETLE_SERVERS");
    if (beetleServers == null) {
      throw new IllegalArgumentException("Missing environment variable BEETLE_SERVERS");
    }

    beetleAdditionalSubscriptionServers = System.getenv("BEETLE_ADDITIONAL_SUBSCRIPTION_SERVERS");
    if (beetleAdditionalSubscriptionServers == null) {
      beetleAdditionalSubscriptionServers = "";
    }

    if (System.getenv().containsKey("BEETLE_REDIS_STATUS_KEY_EXPIRY_INTERVAL")) {
      beetleRedisStatusKeyExpiryInterval =
          Integer.parseInt(System.getenv("BEETLE_REDIS_STATUS_KEY_EXPIRY_INTERVAL"));
    } else {
      beetleRedisStatusKeyExpiryInterval = 0;
    }

    systemName = System.getenv("BEETLE_SYSTEM_NAME");
  }

  public String getBeetleRedisServer() {
    return beetleRedisServer;
  }

  public String getBeetleServers() {
    return beetleServers;
  }

  public String getBeetleAdditionalSubscriptionServers() {
    return beetleAdditionalSubscriptionServers;
  }

  public int getBeetleRedisStatusKeyExpiryInterval() {
    return beetleRedisStatusKeyExpiryInterval;
  }

  public String getSystemName() {
    return systemName;
  }

  public int getMutexExpiration() {
    return 2 * handlerTimeout;
  }

  public int getMaxhandlerExecutionAttemptsDelay() {
    return 2 * handlerExecutionAttemptsDelay;
  }

  public long getMaxHandlerExecutionAttempts() {
    return maxHandlerExecutionAttempts;
  }

  public long getHandlerTimeout() {
    return handlerTimeout;
  }

  public long getExceptionLimit() {
    return exceptionLimit;
  }

  public int getHandlerExecutionAttemptsDelay() {
    return handlerExecutionAttemptsDelay;
  }

  public int getDeadLetteringMsgTtl() {
    return deadLetteringMsgTtl;
  }

  public String getBeetlePolicyExchangeName() {
    return beetlePolicyExchangeName;
  }

  public String getBeetlePolicyUpdatesQueueName() {
    return beetlePolicyUpdatesQueueName;
  }

  public String getBeetlePolicyUpdatesRoutingKey() {
    return beetlePolicyUpdatesRoutingKey;
  }

  public boolean isDeadLetteringEnabled() {
    return deadLetteringEnabled;
  }

  public boolean isLazyQueuesEnabled() {
    return lazyQueuesEnabled;
  }
}
