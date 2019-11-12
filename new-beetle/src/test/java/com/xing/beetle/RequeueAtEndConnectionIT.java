package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.xing.beetle.amqp.RequeueAtEndConnection;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RequeueAtEndConnectionIT {

  private static final String QUEUE = "test-queue";
  private static final int NUMBER_OF_MESSAGES = 10;

  @Container private final RabbitMQContainer container = new RabbitMQContainer();

  @ParameterizedTest
  @CsvSource({
    "-1,false,1,1.0",
    "-1,true,2,0.5",
    "0,true,2,1.0",
    "0,false,1,1.0",
    "99999999,true,1,1.0"
  })
  void test(long delay, boolean requeue, int expectedMessageFactor, BigDecimal expectedBodyeDiff)
      throws Exception {
    int processMessageCount = 0;
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(container.getContainerIpAddress());
    factory.setPort(container.getAmqpPort());
    Channel channel = new RequeueAtEndConnection(factory.newConnection()).createChannel();

    channel.queueDeclare(
        QUEUE, false, false, true, Map.of(BeetleHeader.REQUEUE_AT_END_DELAY, delay));
    for (byte i = 0; i < NUMBER_OF_MESSAGES; i++) {
      channel.basicPublish("", QUEUE, null, new byte[] {i});
    }
    GetResponse msg;
    BigDecimal lastValue = BigDecimal.ZERO;
    while ((msg = channel.basicGet(QUEUE, false)) != null) {
      processMessageCount++;
      byte body = msg.getBody()[0];
      Assertions.assertEquals(lastValue.byteValue(), body);
      lastValue =
          lastValue.add(expectedBodyeDiff).remainder(BigDecimal.valueOf(NUMBER_OF_MESSAGES));

      if (msg.getEnvelope().isRedeliver()
          || msg.getProps().getHeaders() != null
              && msg.getProps().getHeaders().containsKey("x-death")) {
        channel.basicAck(msg.getEnvelope().getDeliveryTag(), false);
      } else {
        channel.basicReject(msg.getEnvelope().getDeliveryTag(), requeue);
      }
    }
    Assertions.assertEquals(expectedMessageFactor * NUMBER_OF_MESSAGES, processMessageCount);
  }
}
