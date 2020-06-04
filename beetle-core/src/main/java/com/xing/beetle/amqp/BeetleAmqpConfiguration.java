package com.xing.beetle.amqp;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Beetle Amqp configuration defined by Environment variables. */
@Configuration
@Profile("!test")
@ConfigurationProperties(prefix = "beetle")
public class BeetleAmqpConfiguration {

  /**
   * system name (used for redis cluster partitioning) (defaults to "system") This is normally
   * defined by the environment variable BEETLE_SYSTEM_NAME.
   */
  @Value("${beetle.system_name:system}")
  private String systemName = "system";

  /**
   * the redis server to use for deduplication either a string like "localhost:6379" (default) or a
   * file that contains the string. use a file if you are using a beetle configuration_client
   * process to update it for automatic redis failover.
   */
  @Value("${beetle.redis_server:virtual/beetle/redis-master-rcc}")
  private String beetleRedisServer = "virtual/beetle/redis-master-rcc";

  /** list of amqp servers to use (defaults to "localhost:5672") */
  @Value("${beetle.servers:localhost:5672}")
  private String beetleServers = "localhost:5672";

  /** list of additional amqp servers to use for subscribers (defaults to "") */
  @Value("${beetle.additional_subscription_servers:}")
  private String beetleAdditionalSubscriptionServers = "";

  /**
   * (seconds )defines how long message tombstones are kept in the deduplication store. This setting
   * helps avoiding duplicate handler executions cause for example by hard application crashes.
   * Setting this to a large value can bring down the Redis deduplication store. When 0, status key
   * will never expire.
   */
  @Value("${beetle.redis_status_key_expiry_interval:0}")
  private int beetleRedisStatusKeyExpiryIntervalSeconds = 0;

  /**
   * policy_exchange_name is the name of the exchange on which to publish messages to set up queue
   * policies. Whenever a queue is declared in the client, either by publisher or consumer, a
   * message with the queue policy parameters is sent to this exchange. (defaults to
   * "beetle-policies")
   */
  @Value("${beetle.policy_exchange_name:beetle-policies}")
  private String beetlePolicyExchangeName = "beetle-policies";

  /** Name of the policy update queue. */
  @Value("${beetle.policy_updates_queue_name:beetle-policy-updates}")
  private String beetlePolicyUpdatesQueueName = "beetle-policy-updates";

  /** Name of the policy update routing key. */
  @Value("${beetle.policy_updates_routing_key:beetle.policy.update}")
  private String beetlePolicyUpdatesRoutingKey = "beetle.policy.update";

  /**
   * how long we should repeatedly retry a redis operation before giving up, with a one second sleep
   * between retries (defaults to 180 seconds). this value needs to be somewhere between the maximum
   * time it takes to auto-switch redis and the smallest handler timeout.
   */
  @Value("${beetle.redis_failover_timeout:180}")
  private int redisFailoverTimeoutSeconds = 180;

  /**
   * In contrast to RabbitMQ 2.x, RabbitMQ 3.x preserves message order when requeing a message. This
   * can lead to # throughput degradation (when rejected messages block the processing of other
   * messages at the head of the queue) in some cases.
   *
   * <p>This setting enables the creation of dead letter queues that mimic the old beetle behaviour
   * on RabbitMQ 3.x. Instead of rejecting messages with "requeue => true", beetle will setup dead
   * letter queues for all queues, will reject messages with "requeue => false", where messages are
   * temporarily moved to the side and are republished to the end of the original queue when they
   * expire in the dead letter queue
   *
   * <p>By default this is turned off and needs to be explicitly enabled.
   */
  @Value("${beetle.dead_lettering_enabled:false}")
  private boolean deadLetteringEnabled = false;

  /**
   * Lazy queues have the advantage of consuming a lot less memory on the broker. For backwards
   * compatibility, they are disabled by default.
   */
  @Value("${beetle.lazy_queues_enabled:false}")
  private boolean lazyQueuesEnabled = false;

  /**
   * the time a message spends in the dead letter queue in milliseconds if dead lettering is
   * enabled, before it is returned to the original queue
   */
  @Value("${beetle.dead_lettering_msg_ttl:1000}")
  private int deadLetteringMsgTtlMs = 1000; // 1 second

  /** the AMQP user to use when connecting to the AMQP servers (defaults to "guest") */
  @Value("${beetle.user:guest}")
  private String user = "guest";

  /** the password to use when connecting to the AMQP servers (defaults to "guest") */
  @Value("${beetle.password:guest}")
  private String password = "guest";

  /** forcefully abort a running handler after this many seconds. */
  @Value("${beetle.handler_timeout:600}")
  private int handlerTimeoutSeconds = 600;

  /** how many times we should try to run a handler before giving up the handler execution */
  @Value("${beetle.handler_execution_attempts:600}")
  private int maxHandlerExecutionAttempts = 1;

  /** how many seconds we should wait before retrying handler execution */
  @Value("${beetle.handler_execution_attempts_delay:10}")
  private int handlerExecutionAttemptsDelaySeconds = 10; // seconds

  /** how many exceptions should be tolerated before giving up the handler execution */
  @Value("${beetle.exception_limit:0}")
  private int exceptionLimit = 0;

  /** lifetime of messages in seconds */
  @Value("${beetle.ttl:86400}")
  private int messageLifetimeSeconds = 24 * 60 * 60;

  public String getBeetleRedisServer() {
    return beetleRedisServer;
  }

  public String getBeetleServers() {
    return beetleServers;
  }

  public String getBeetleAdditionalSubscriptionServers() {
    return beetleAdditionalSubscriptionServers;
  }

  public int getBeetleRedisStatusKeyExpiryIntervalSeconds() {
    return beetleRedisStatusKeyExpiryIntervalSeconds;
  }

  public String getSystemName() {
    return systemName;
  }

  public int getMutexExpiration() {
    return 2 * handlerTimeoutSeconds;
  }

  public int getMaxhandlerExecutionAttemptsDelay() {
    return 2 * handlerExecutionAttemptsDelaySeconds;
  }

  public long getMaxHandlerExecutionAttempts() {
    return maxHandlerExecutionAttempts;
  }

  public long getHandlerTimeoutSeconds() {
    return handlerTimeoutSeconds;
  }

  public long getExceptionLimit() {
    return exceptionLimit;
  }

  public int getHandlerExecutionAttemptsDelaySeconds() {
    return handlerExecutionAttemptsDelaySeconds;
  }

  public int getDeadLetteringMsgTtlMs() {
    return deadLetteringMsgTtlMs;
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

  public int getRedisFailoverTimeoutSeconds() {
    return redisFailoverTimeoutSeconds;
  }

  void setRedisFailoverTimeoutSeconds(int redisFailoverTimeoutSeconds) {
    this.redisFailoverTimeoutSeconds = redisFailoverTimeoutSeconds;
  }

  public int getMessageLifetimeSeconds() {
    return messageLifetimeSeconds;
  }

  public void setMessageLifetimeSeconds(int messageLifetimeSeconds) {
    this.messageLifetimeSeconds = messageLifetimeSeconds;
  }

  public void setBeetleRedisServer(String beetleRedisServer) {
    this.beetleRedisServer = beetleRedisServer;
  }
}
