package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.xing.beetle.amqp.MultiPlexingConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MultiPlexingConnectionIT {

  private static final String QUEUE = "test-queue";
  private static final int NUMBER_OF_MESSAGES = 10;

  @Container RabbitMQContainer container = new RabbitMQContainer();

  @ParameterizedTest
  @CsvSource({
    "GET,AUTO",
    "GET,SINGLE",
    "GET,MULTIPLE",
    "CONSUME,AUTO",
    "CONSUME,SINGLE",
    "CONSUME,MULTIPLE"
  })
  void test(ChannelReadMode mode, MessageAcknowledgementStrategy strategy) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(container.getContainerIpAddress());
    factory.setPort(container.getAmqpPort());
    MultiPlexingConnection connection = new MultiPlexingConnection(factory.newConnection());
    Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE, false, false, false, null);
    for (byte i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", QUEUE, null, new byte[] {i});
    }

    int messageCount = mode.readAck(channel, QUEUE, strategy, NUMBER_OF_MESSAGES);
    Assertions.assertEquals(NUMBER_OF_MESSAGES, messageCount);

    channel.close();
    channel = connection.createChannel();
    Assertions.assertEquals(0, channel.messageCount(QUEUE));
  }
}
