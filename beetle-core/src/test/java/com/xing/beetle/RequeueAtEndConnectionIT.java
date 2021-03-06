package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.RequeueAtEndConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;

import static org.mockito.Mockito.when;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RequeueAtEndConnectionIT {

  private static final String QUEUE_PREFIX = "test-queue-";
  private static final int NUMBER_OF_MESSAGES = 10;

  private RabbitMQContainer container;

  public RequeueAtEndConnectionIT() {
    TestContainerProvider.startContainers();
    container = TestContainerProvider.rabbitMQContainers.get(0);
  }

  @ParameterizedTest
  @CsvSource({
    "q1,-1,false,false,1,1.0",
    "q2,-1,false,true,2,0.5",
    "q3,0,false,false,2,1.0",
    "q4,0,false,true,2,0.5",
    "q5,0,true,true,2,1.0",
    "q6,0,true,false,1,1.0",
    "q7,99999999,true,true,1,1.0"
  })
  void test(
      String queueSuffix,
      int requeueDelay,
      boolean revertRequeue,
      boolean requeue,
      int expectedMessageFactor,
      BigDecimal expectedBodyDiff)
      throws Exception {
    int processMessageCount = 0;
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("127.0.0.1");
    factory.setPort(container.getAmqpPort());

    BeetleAmqpConfiguration beetleAmqpConfiguration = beetleAmqpConfiguration();
    when(beetleAmqpConfiguration.getBeetleServers())
        .thenReturn("127.0.0.1:" + container.getAmqpPort());
    if (requeueDelay >= 0) {
      when(beetleAmqpConfiguration.getDeadLetteringMsgTtlMs()).thenReturn(requeueDelay);
      when(beetleAmqpConfiguration.isDeadLetteringEnabled()).thenReturn(true);
    } else {
      when(beetleAmqpConfiguration.isDeadLetteringEnabled()).thenReturn(false);
    }
    try (Connection connection =
        new RequeueAtEndConnection(
            factory.newConnection(), beetleAmqpConfiguration, revertRequeue)) {
      Channel channel = connection.createChannel();
      String queueName = QUEUE_PREFIX + queueSuffix;
      channel.queueDeclare(queueName, false, false, true, null);
      for (byte i = 0; i < NUMBER_OF_MESSAGES; i++) {
        channel.basicPublish("", queueName, null, new byte[] {i});
      }
      GetResponse msg;
      BigDecimal lastValue = BigDecimal.ZERO;
      while ((msg = channel.basicGet(queueName, false)) != null) {
        processMessageCount++;
        byte body = msg.getBody()[0];
        Assertions.assertEquals(lastValue.byteValue(), body);
        lastValue =
            lastValue.add(expectedBodyDiff).remainder(BigDecimal.valueOf(NUMBER_OF_MESSAGES));

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

  BeetleAmqpConfiguration beetleAmqpConfiguration() {
    BeetleAmqpConfiguration beetleAmqpConfiguration = Mockito.mock(BeetleAmqpConfiguration.class);

    when(beetleAmqpConfiguration.getHandlerTimeoutSeconds()).thenReturn(1L);
    when(beetleAmqpConfiguration.getMutexExpiration()).thenReturn(2);
    when(beetleAmqpConfiguration.getExceptionLimit()).thenReturn(3L);
    when(beetleAmqpConfiguration.getMaxHandlerExecutionAttempts()).thenReturn(3L);
    when(beetleAmqpConfiguration.getBeetleRedisStatusKeyExpiryIntervalSeconds()).thenReturn(0);
    when(beetleAmqpConfiguration.getHandlerExecutionAttemptsDelaySeconds()).thenReturn(1);
    when(beetleAmqpConfiguration.getMaxhandlerExecutionAttemptsDelay()).thenReturn(2);
    when(beetleAmqpConfiguration.getDeadLetteringMsgTtlMs()).thenReturn(10);

    when(beetleAmqpConfiguration.getBeetlePolicyExchangeName()).thenReturn("beetle-policies");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesQueueName())
        .thenReturn("beetle-policy-updates");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesRoutingKey())
        .thenReturn("beetle.policy.update");

    return beetleAmqpConfiguration;
  }
}
