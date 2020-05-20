package com.xing.beetle;

import java.util.Arrays;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.util.ExceptionSupport;

import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class BaseBeetleIT {

  @Container public static final RabbitMQContainer rmq1 = new RabbitMQContainer();
  @Container public static final RabbitMQContainer rmq2 = new RabbitMQContainer();
  @Container public static final RabbitMQContainer rmq3 = new RabbitMQContainer();

  public static final RabbitMQContainer[] rmq = {rmq1, rmq2, rmq3};

  static final IntFunction<AMQP.BasicProperties> REDUNDANT =
      r ->
          new AMQP.BasicProperties.Builder()
              .headers(Map.of(BeetleHeader.PUBLISH_REDUNDANCY, r))
              .build();

  private static Address addressOf(RabbitMQContainer container) {
    String amqpUrl = container.getAmqpUrl();
    return Address.parseAddress(amqpUrl.substring(7));
  }

  static Connection createConnection(BeetleConnectionFactory factory, int count) throws Exception {
    Address[] addresses =
        Arrays.stream(rmq, 0, count).map(BaseBeetleIT::addressOf).toArray(Address[]::new);
    return factory.newConnection(addresses);
  }

  static Stream<Connection> createConnections(ConnectionFactory factory, int count)
      throws Exception {
    return Arrays.stream(rmq, 0, count)
        .map(rabbitMQContainer -> createConnection(factory, rabbitMQContainer));
  }

  static Connection createConnection(ConnectionFactory factory, RabbitMQContainer container) {
    try {
      return factory.newConnection(new Address[] {addressOf(container)});
    } catch (Exception e) {
      return ExceptionSupport.sneakyThrow(e);
    }
  }
}
