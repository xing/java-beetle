package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.MultiPlexingConnection;
import com.xing.beetle.dedup.spi.Deduplicator;
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

  @ParameterizedTest(name = "MplexConn {0}/{1}")
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

    MultiPlexingConnection connection =
        new MultiPlexingConnection(
            factory.newConnection(),
            new Deduplicator() {
              @Override
              public boolean tryAcquireMutex(String messageId, int secondsToExpire) {
                return false;
              }

              @Override
              public void releaseMutex(String messageId) {}

              @Override
              public void complete(String messageId) {}

              @Override
              public boolean completed(String messageId) {
                return false;
              }

              @Override
              public boolean delayed(String messageId) {
                return false;
              }

              @Override
              public void setDelay(String messageId, long timestamp) {}

              @Override
              public long incrementAttempts(String messageId) {
                return 0;
              }

              @Override
              public long incrementExceptions(String messageId) {
                return 0;
              }

              @Override
              public long incrementAckCount(String messageId) {
                return 0;
              }

              @Override
              public void deleteKeys(String messageId) {}

              @Override
              public boolean initKeys(String messageId, long expirationTime) {
                return false;
              }

              @Override
              public BeetleAmqpConfiguration getBeetleAmqpConfiguration() {
                return null;
              }
            },
            true);
    Channel channel = connection.createChannel();

    String queue = String.format("%s-%s-%s", QUEUE, mode, strategy);
    channel.queueDeclare(queue, false, false, false, null);

    for (byte i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", queue, null, new byte[] {i});
    }

    int readMessageCount = mode.readAck(channel, queue, strategy, NUMBER_OF_MESSAGES);
    Assertions.assertEquals(NUMBER_OF_MESSAGES, readMessageCount);

    for (byte i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", queue, null, new byte[] {i});
    }

    AMQP.Queue.PurgeOk purgeOk = channel.queuePurge(queue);
    Assertions.assertEquals(NUMBER_OF_MESSAGES, purgeOk.getMessageCount());

    channel.close();
    channel = connection.createChannel();
    Assertions.assertEquals(0, channel.messageCount(queue));
  }
}
