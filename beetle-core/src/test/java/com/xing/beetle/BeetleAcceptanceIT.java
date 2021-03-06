package com.xing.beetle;

import com.rabbitmq.client.*;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.util.ExceptionSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.xing.beetle.Assertions.assertEventualLength;

@Testcontainers
class BeetleAcceptanceIT extends BaseBeetleIT {

  private String redisServer = "";

  private GenericContainer<?> redis;

  private static String getRedisAddress(GenericContainer<?> redisContainer) {
    return "127.0.0.1:" + redisContainer.getFirstMappedPort();
  }

  public BeetleAcceptanceIT() {
    TestContainerProvider.startContainers();
    redis = TestContainerProvider.redis;
    redisServer = getRedisAddress(redis);
  }

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  void testRedundantPublish_channelLevelDeduplication(int containers) throws Exception {
    BeetleAmqpConfiguration beetleAmqpConfiguration = beetleAmqpConfiguration(containers);

    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    String queue = String.format("channelLevelDeduplication-%d", containers);

    channel.queueDeclare(queue, false, false, false, null);
    channel.basicPublish("", queue, BaseBeetleIT.REDUNDANT.apply(1), "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    channel.basicConsume(queue, true, (tag, msg) -> messages.add(msg), System.out::println);

    assertEventualLength(messages, 1, 1000);
    messages.clear();

    channel.basicPublish("", queue, BaseBeetleIT.REDUNDANT.apply(2), "test2".getBytes());

    assertEventualLength(messages, 1, 1000);
  }

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  void testRedundantPublish_channelLevelDeduplication_deadLetteringEnabled_exceptions(
      int containers) throws Exception {
    BeetleAmqpConfiguration beetleAmqpConfiguration = beetleAmqpConfiguration(containers);
    beetleAmqpConfiguration.setDeadLetteringEnabled(true);
    beetleAmqpConfiguration.setDeadLetteringMsgTtlMs(1000);

    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration);

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    String queue = String.format("deadLetteringEnabled_exceptions-%d", containers);

    channel.queueDeclare(queue, false, false, false, null);
    AMQP.BasicProperties basicProperties = BaseBeetleIT.REDUNDANT.apply(2);
    AMQP.BasicProperties properties =
        basicProperties.builder().messageId(UUID.randomUUID().toString()).build();

    channel.basicPublish("", queue, properties, "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    DeliverCallback deliverCallback =
        (tag, msg) -> {
          messages.add(msg);
          throw new NullPointerException("");
        };
    channel.basicConsume(queue, true, deliverCallback, System.out::println);

    assertEventualLength(messages, 3, 10000);

    connection.close();
  }

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  void testRedundantPublish_channelLevelDeduplication_deadLetteringEnabled_timeouts(int containers)
      throws Exception {
    BeetleAmqpConfiguration beetleAmqpConfiguration = beetleAmqpConfiguration(containers);
    beetleAmqpConfiguration.setDeadLetteringEnabled(true);
    beetleAmqpConfiguration.setDeadLetteringMsgTtlMs(100);

    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration);

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    String queue = String.format("deadLetteringEnabled_timeouts-%d", containers);

    channel.queueDeclare(queue, false, false, false, null);
    AMQP.BasicProperties basicProperties = BaseBeetleIT.REDUNDANT.apply(2);
    AMQP.BasicProperties properties =
        basicProperties.builder().messageId(UUID.randomUUID().toString()).build();

    channel.basicPublish("", queue, properties, "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    DeliverCallback deliverCallback =
        (tag, msg) -> {
          messages.add(msg);
          // simulate timeout
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            ExceptionSupport.sneakyThrow(e);
          }
        };
    channel.basicConsume(queue, true, deliverCallback, System.out::println);
    assertEventualLength(messages, 3, 10000);
    connection.close();
  }

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  void testRedundantPublish_channelLevelDeduplication_deadLetteringDisabled_timeouts(int containers)
      throws Exception {
    BeetleAmqpConfiguration beetleAmqpConfiguration = beetleAmqpConfiguration(containers);
    beetleAmqpConfiguration.setDeadLetteringEnabled(false);
    beetleAmqpConfiguration.setDeadLetteringMsgTtlMs(1000);

    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration);

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    String queue = String.format("deadLetteringDisabled_timeouts-%d", containers);

    channel.queueDeclare(queue, false, false, false, null);
    AMQP.BasicProperties basicProperties = BaseBeetleIT.REDUNDANT.apply(2);
    AMQP.BasicProperties properties =
        basicProperties.builder().messageId(UUID.randomUUID().toString()).build();

    channel.basicPublish("", queue, properties, "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    DeliverCallback deliverCallback =
        (tag, msg) -> {
          messages.add(msg);
          // simulate timeout
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            ExceptionSupport.sneakyThrow(e);
          }
        };
    channel.basicConsume(queue, true, deliverCallback, System.out::println);
    assertEventualLength(messages, 3, 10000);
    connection.close();
  }

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  void testRedundantPublish_channelLevelDeduplication_deadLetteringDisabled_exceptions(
      int containers) throws Exception {
    BeetleAmqpConfiguration beetleAmqpConfiguration = beetleAmqpConfiguration(containers);
    beetleAmqpConfiguration.setDeadLetteringEnabled(false);
    beetleAmqpConfiguration.setDeadLetteringMsgTtlMs(1000);

    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration);

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    String queue = String.format("deadLetteringDisabled_exceptions-%d", containers);

    channel.queueDeclare(queue, false, false, false, null);
    AMQP.BasicProperties basicProperties = BaseBeetleIT.REDUNDANT.apply(2);
    AMQP.BasicProperties properties =
        basicProperties.builder().messageId(UUID.randomUUID().toString()).build();

    channel.basicPublish("", queue, properties, "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    DeliverCallback deliverCallback =
        (tag, msg) -> {
          messages.add(msg);
          throw new IllegalStateException("");
        };
    channel.basicConsume(queue, true, deliverCallback, System.out::println);

    assertEventualLength(messages, 3, 10000);

    connection.close();
  }

  BeetleAmqpConfiguration beetleAmqpConfiguration(int containers) {

    List<String> rabbitAddresses =
        TestContainerProvider.rabbitMQContainers.stream()
            .map(rabbit -> "127.0.0.1:" + rabbit.getFirstMappedPort())
            .limit(containers)
            .collect(Collectors.toList());

    BeetleAmqpConfiguration beetleAmqpConfiguration = new BeetleAmqpConfiguration();
    beetleAmqpConfiguration.setBeetleServers(String.join(",", rabbitAddresses));
    beetleAmqpConfiguration.setBeetleRedisServer(redisServer);
    beetleAmqpConfiguration.setHandlerTimeoutSeconds(1);
    beetleAmqpConfiguration.setMaxHandlerExecutionAttempts(3);
    beetleAmqpConfiguration.setHandlerExecutionAttemptsDelaySeconds(1);
    beetleAmqpConfiguration.setExceptionLimit(3);

    return beetleAmqpConfiguration;
  }
}
