package com.xing.beetle;

import static org.junit.Assert.assertEquals;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.xing.beetle.amqp.BeetleConnection;
import com.xing.beetle.testcontainers.ContainerLifecycle;
import com.xing.beetle.testcontainers.Containers;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class BeetleConnectionIT extends BaseBeetleIT {

  private static final int NUMBER_OF_MESSAGES = 10;

  @ExtendWith(ContainerLifecycle.class)
  @ParameterizedTest
  @MethodSource("generateTestParameters")
  void testReadAck(
      @Containers RabbitMQContainer[] containers,
      ChannelReadMode mode,
      MessageAcknowledgementStrategy strategy)
      throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    Stream<Connection> connections = createConnections(factory, containers);
    BeetleConnection beetleConnection =
        new BeetleConnection(connections.collect(Collectors.toList()));
    Channel channel = beetleConnection.createChannel();

    channel.queueDeclare(QUEUE, false, false, false, null);
    int redundancy = 2;
    for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", QUEUE, REDUNDANT.apply(redundancy), "test1".getBytes());
    }

    long expectedNumberOfMessages = NUMBER_OF_MESSAGES * Math.min(containers.length, redundancy);
    int messageCount = mode.readAck(channel, QUEUE, strategy, expectedNumberOfMessages);
    assertEquals(expectedNumberOfMessages, messageCount);

    channel.close();
    channel = beetleConnection.createChannel();
    assertEquals(0, channel.messageCount(QUEUE));
  }

  @ExtendWith(ContainerLifecycle.class)
  @ParameterizedTest
  @MethodSource("generateTestParametersNack")
  void testReadNack(
      @Containers RabbitMQContainer[] containers,
      ChannelReadMode mode,
      MessageAcknowledgementStrategy strategy)
      throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    Stream<Connection> connections = createConnections(factory, containers);
    BeetleConnection beetleConnection =
        new BeetleConnection(connections.collect(Collectors.toList()));
    Channel channel = beetleConnection.createChannel();

    channel.queueDeclare(QUEUE, false, false, false, null);
    int redundancy = 2;
    for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", QUEUE, REDUNDANT.apply(redundancy), "test1".getBytes());
    }

    long numberOfMessagesToNack = NUMBER_OF_MESSAGES * Math.min(containers.length, redundancy);
    int messageCount = mode.readNack(channel, QUEUE, strategy, true, numberOfMessagesToNack);
    assertEquals(2 * numberOfMessagesToNack, messageCount);

    channel.close();
    channel = beetleConnection.createChannel();
    assertEquals(0, channel.messageCount(QUEUE));
  }

  static Object[] add(Object[] arr, Object element) {
    Object[] result = Arrays.copyOf(arr, arr.length + 1);
    result[arr.length] = element;
    return result;
  }

  static Stream<Object[]> generateTestParameters() {
    return IntStream.rangeClosed(1, 3)
        .mapToObj(cc -> add(new Object[0], cc))
        .flatMap(args -> Stream.of(ChannelReadMode.values()).map(rm -> add(args, rm)))
        .flatMap(
            args -> Stream.of(MessageAcknowledgementStrategy.values()).map(rm -> add(args, rm)));
  }

  static Stream<Object[]> generateTestParametersNack() {
    return generateTestParameters()
        .filter(objects -> Arrays.stream(objects).noneMatch(o -> o.toString().contains("AUTO")));
  }

  @Test
  void testParams() {
    assertEquals(18, generateTestParameters().count());
  }
}
