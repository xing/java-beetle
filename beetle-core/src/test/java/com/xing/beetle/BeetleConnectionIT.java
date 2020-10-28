package com.xing.beetle;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleConnection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class BeetleConnectionIT extends BaseBeetleIT {
  private static final int NUMBER_OF_MESSAGES = 5;
  private Channel singleBrokerChannel;
  private Channel twoBrokersChannel;

  public BeetleConnectionIT() {
    TestContainerProvider.startContainers();
    ConnectionFactory factory = new ConnectionFactory();
    List<Connection> connections;
    try {
      connections =
          createConnections(TestContainerProvider.rabbitMQContainers, factory, 2)
              .collect(Collectors.toList());
      twoBrokersChannel = createChannel(connections);
      singleBrokerChannel = createChannel(Collections.singletonList(connections.get(0)));
    } catch (Exception ignored) {
    }
  }

  public Channel createChannel(List<Connection> connections) throws IOException {
    BeetleConnection beetleConnection =
        new BeetleConnection(connections, new BeetleAmqpConfiguration());
    return beetleConnection.createChannel();
  }

  @ParameterizedTest(name = "Read ACK {0} {1} {2}")
  @MethodSource("generateTestParameters")
  @DisplayName("Read ACK")
  void testReadAck(int containers, ChannelReadMode mode, MessageAcknowledgementStrategy strategy)
      throws Exception {

    Channel channel = containers == 1 ? singleBrokerChannel : twoBrokersChannel;
    String queue = String.format("%d-%s-%s", containers, mode, strategy);

    channel.queueDeclare(queue, false, false, false, null);
    int redundancy = 2;

    for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", queue, REDUNDANT.apply(redundancy), "test1".getBytes());
    }

    long expectedNumberOfMessages = NUMBER_OF_MESSAGES * Math.min(containers, redundancy);
    int messageCount = mode.readAck(channel, queue, strategy, expectedNumberOfMessages);
    assertEquals(expectedNumberOfMessages, messageCount);
    assertEquals(0, channel.messageCount(queue));
  }

  @ParameterizedTest(name = "Read NACK {0} {1} {2}")
  @MethodSource("generateTestParametersNack")
  void testReadNack(int containers, ChannelReadMode mode, MessageAcknowledgementStrategy strategy)
      throws Exception {

    ConnectionFactory factory = new ConnectionFactory();
    Stream<Connection> connections =
        createConnections(TestContainerProvider.rabbitMQContainers, factory, containers);
    BeetleConnection beetleConnection =
        new BeetleConnection(
            connections.collect(Collectors.toList()), new BeetleAmqpConfiguration());
    Channel channel = beetleConnection.createChannel();
    String queue = String.format("%d-%s-%s", containers, mode, strategy);

    channel.queueDeclare(queue, false, false, false, null);
    int redundancy = 2;
    for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", queue, REDUNDANT.apply(redundancy), "test1".getBytes());
    }

    long numberOfMessagesToNack = NUMBER_OF_MESSAGES * Math.min(containers, redundancy);
    int messageCount = mode.readNack(channel, queue, strategy, true, numberOfMessagesToNack);
    assertEquals(2 * numberOfMessagesToNack, messageCount);

    channel.close();
    channel = beetleConnection.createChannel();
    assertEquals(0, channel.messageCount(queue));
    channel.close();
    beetleConnection.close();
  }

  static Object[] add(Object[] arr, Object element) {
    Object[] result = Arrays.copyOf(arr, arr.length + 1);
    result[arr.length] = element;
    return result;
  }

  /**
   * generate permutations of number of connected RMQ servers (1-2), read mode and acknowledgment
   * strategies
   */
  static Stream<Object[]> generateTestParameters() {
    return IntStream.rangeClosed(1, 2)
        .mapToObj(cc -> add(new Object[0], cc))
        .flatMap(args -> Stream.of(ChannelReadMode.values()).map(rm -> add(args, rm)))
        .flatMap(
            args -> Stream.of(MessageAcknowledgementStrategy.values()).map(rm -> add(args, rm)));
  }

  /**
   * generate permutations of number of connected RMQ servers (1-2), read mode and acknowledgment
   * strategies excluding automatic ACK
   */
  static Stream<Object[]> generateTestParametersNack() {
    return generateTestParameters()
        .filter(objects -> Arrays.stream(objects).noneMatch(o -> o.toString().contains("AUTO")));
  }
}
