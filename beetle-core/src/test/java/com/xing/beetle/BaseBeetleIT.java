package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.xing.beetle.util.ExceptionSupport;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Stream;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BaseBeetleIT {

  static final IntFunction<AMQP.BasicProperties> REDUNDANT =
      r ->
          new AMQP.BasicProperties.Builder()
              .headers(Map.of(BeetleHeader.PUBLISH_REDUNDANCY, r))
              .build();

  private static Address addressOf(RabbitMQContainer container) {
    String amqpUrl = container.getAmqpUrl();
    return Address.parseAddress(amqpUrl.substring(7));
  }

  static Stream<Connection> createConnections(
      List<RabbitMQContainer> rabbitMQContainers, ConnectionFactory factory, int count) {
    return rabbitMQContainers.stream()
        .limit(count)
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
