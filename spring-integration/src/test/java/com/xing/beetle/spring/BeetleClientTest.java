package com.xing.beetle.spring;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.xing.beetle.BeetleHeader;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;

import com.xing.testing.TestFailureLogger;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.GenericContainer;

/**
 * Full blown beetle client test with spring integration (RabbitListener) and deduplication (with
 * Redis) where dead lettering disabled.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ExtendWith(MockitoExtension.class)
@TestExecutionListeners(
    value = {TestFailureLogger.class},
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
@SpringBootTest
@DirtiesContext
public class BeetleClientTest {

  @Autowired private RabbitTemplate rabbitTemplate;
  @Autowired private MessageHandlingService service;

  private static String beetleServers;
  private static String redisServer;

  static {
    GenericContainer redis = new GenericContainer("redis:3.0.2").withExposedPorts(6379);
    redis.start();

    List<GenericContainer> rabbitBrokers =
        IntStream.range(0, 2)
            .mapToObj(i -> new GenericContainer("rabbitmq:3.5.3").withExposedPorts(5672))
            .collect(Collectors.toList());
    rabbitBrokers.forEach(GenericContainer::start);

    List<String> rabbitAddresses =
        rabbitBrokers.stream()
            .map(rabbit -> rabbit.getContainerIpAddress() + ":" + rabbit.getFirstMappedPort())
            .collect(Collectors.toList());

    beetleServers = String.join(",", rabbitAddresses);
    redisServer =
        String.join(
            ":", new String[] {redis.getContainerIpAddress(), redis.getFirstMappedPort() + ""});

    System.setProperty("test.deadLetterEnabled", "false");
  }

  private void sendRedundantMessage(String routingKey, int redundancy, String messageId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, redundancy);
    props.setMessageId(messageId);
    Message message = new Message("foo".getBytes(), props);
    rabbitTemplate.send("", routingKey, message);
  }

  @Test
  public void handleSuccessfully() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueSucceed", 2, messageId);

    String messageId2 = UUID.randomUUID().toString();
    sendRedundantMessage("QueueSucceed", 2, messageId2);

    service.assertCounts(messageId, 1, 0, 0, 10000);
    service.assertCounts(messageId2, 1, 0, 0, 10000);
    // make sure that queue for policy is declared and working
    assertFalse(service.queuePolicyMessages.isEmpty());
  }

  @Test
  public void testTemplateReceive() {

    for (int i = 0; i < 10; i++) {
      rabbitTemplate.convertAndSend("QueueTemplate", "message");
    }

    int count = 0;

    for (int i = 0; i < 10; i++) {
      Message m = rabbitTemplate.receive("QueueTemplate", 50);
      if (m != null) {
        assertEquals("message", new String(m.getBody()));
      }
      count++;
    }

    assertEquals(10, count);
  }

  public void waitForMessageDelivery(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void throwExceptionExceedExceptionLimit() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithError", 2, messageId);
    waitForMessageDelivery(8000);
    // exception limit is 3
    service.assertCounts(messageId, 3, 0, 1, 10000);
  }

  @Test
  public void timeoutExceedExceptionLimit() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithTimeout", 2, messageId);
    // exception limit is 3
    service.assertCounts(messageId, 3, 0, 1, 10000);
  }

  @Test
  public void firstTimeoutThenSucceed() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithTimeoutThenSucceed", 2, messageId);
    service.assertCounts(messageId, 1, 0, 0, 10000);
  }

  @Test
  public void firstThrowExceptionThenHandle() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithErrorThenSucceed", 2, messageId);
    service.assertCounts(messageId, 1, 0, 0, 10000);
  }

  public static class MessageHandlingService extends RecordingMessageHandler {

    @RabbitListener(queues = "QueueSucceed")
    public void handle(Message message) {
      result.add(message.getMessageProperties().getMessageId());
    }

    @RabbitListener(queues = "QueueSucceed")
    public void handle2(Message message) {
      result.add(message.getMessageProperties().getMessageId());
    }

    @RabbitListener(queues = "QueueWithErrorThenSucceed")
    public void handleWithErrorThenSucceed(Message message) {
      synchronized (result) {
        if (!result.contains(message.getMessageProperties().getMessageId())) {
          result.add(message.getMessageProperties().getMessageId());
          throw new IllegalStateException(
              "message handling failed for " + message.getMessageProperties().getMessageId());
        }
      }
    }

    @RabbitListener(queues = "QueueWithError")
    public void handleWithError(Message message) {
      log.log(System.Logger.Level.DEBUG, message.getMessageProperties());
      if (message.getMessageProperties().isRedelivered()) {
        redelivered.add(message.getMessageProperties().getMessageId());
      }
      if (message.getMessageProperties().getHeader("x-death") != null) {
        deadLettered.add(message.getMessageProperties().getMessageId());
      }
      result.add(message.getMessageProperties().getMessageId());
      throw new IllegalStateException(
          "message handling failed for " + message.getMessageProperties().getMessageId());
    }

    @RabbitListener(queues = "QueueWithTimeout")
    public void handleWithTimeout(Message message) throws InterruptedException {
      log.log(System.Logger.Level.DEBUG, message.getMessageProperties());
      if (message.getMessageProperties().isRedelivered()) {
        redelivered.add(message.getMessageProperties().getMessageId());
      }
      if (message.getMessageProperties().getHeader("x-death") != null) {
        deadLettered.add(message.getMessageProperties().getMessageId());
      }
      result.add(message.getMessageProperties().getMessageId());
      Thread.sleep(1100);
    }

    @RabbitListener(queues = "QueueWithTimeoutThenSucceed")
    public void handleWithTimeoutThenSucceed(Message message) throws InterruptedException {
      synchronized (result) {
        if (!result.contains(message.getMessageProperties().getMessageId())) {
          result.add(message.getMessageProperties().getMessageId());
          Thread.sleep(2000);
        }
      }
    }

    @RabbitListener(queues = "beetle-policy-updates")
    public void receiveQueuePolicyMessages(Message message) {
      queuePolicyMessages.add(message.getMessageProperties().getMessageId());
    }
  }

  @Configuration
  @ConditionalOnProperty(value = "test.deadLetterEnabled", havingValue = "false")
  @EnableRabbit
  @EnableTransactionManagement
  @SpringBootConfiguration
  @EnableAutoConfiguration
  public static class EnableRabbitConfig {

    @Bean
    public MessageHandlingService handlingService() {
      return new MessageHandlingService();
    }

    @Bean
    public org.springframework.amqp.core.Queue queueWithTimeoutThenSucceed() {
      return new org.springframework.amqp.core.Queue("QueueWithTimeoutThenSucceed");
    }

    @Bean
    public org.springframework.amqp.core.Queue queueWithErrorThenSucceed() {
      return new org.springframework.amqp.core.Queue("QueueWithErrorThenSucceed");
    }

    @Bean
    public org.springframework.amqp.core.Queue queueSucceed() {
      return new org.springframework.amqp.core.Queue("QueueSucceed");
    }

    @Bean
    public org.springframework.amqp.core.Queue queueWithError() {
      return new org.springframework.amqp.core.Queue("QueueWithError");
    }

    @Bean
    public org.springframework.amqp.core.Queue queueWithTimeout() {
      return new org.springframework.amqp.core.Queue("QueueWithTimeout");
    }

    @Bean
    public org.springframework.amqp.core.Queue queueTemplate() {
      return new org.springframework.amqp.core.Queue("QueueTemplate");
    }

    @Bean
    @Primary
    public BeetleAmqpConfiguration beetleAmqpConfiguration() {
      BeetleAmqpConfiguration beetleAmqpConfiguration = Mockito.mock(BeetleAmqpConfiguration.class);

      when(beetleAmqpConfiguration.getBeetleRedisServer()).thenReturn(redisServer);
      when(beetleAmqpConfiguration.getBeetleServers()).thenReturn(beetleServers);
      when(beetleAmqpConfiguration.getSystemName()).thenReturn("system");
      when(beetleAmqpConfiguration.getHandlerTimeoutSeconds()).thenReturn(1L);
      when(beetleAmqpConfiguration.getMutexExpiration()).thenReturn(2);
      when(beetleAmqpConfiguration.getExceptionLimit()).thenReturn(3L);
      when(beetleAmqpConfiguration.getMaxHandlerExecutionAttempts()).thenReturn(3L);
      when(beetleAmqpConfiguration.getBeetleRedisStatusKeyExpiryIntervalSeconds()).thenReturn(0);
      when(beetleAmqpConfiguration.getHandlerExecutionAttemptsDelaySeconds()).thenReturn(1);
      when(beetleAmqpConfiguration.getMaxhandlerExecutionAttemptsDelay()).thenReturn(2);
      when(beetleAmqpConfiguration.getDeadLetteringMsgTtlMs()).thenReturn(100);
      when(beetleAmqpConfiguration.isDeadLetteringEnabled()).thenReturn(false);
      when(beetleAmqpConfiguration.getRedisFailoverTimeoutSeconds()).thenReturn(3);
      when(beetleAmqpConfiguration.getMessageLifetimeSeconds()).thenReturn(10000);

      when(beetleAmqpConfiguration.getBeetlePolicyExchangeName()).thenReturn("beetle-policies");
      when(beetleAmqpConfiguration.getBeetlePolicyUpdatesQueueName())
          .thenReturn("beetle-policy-updates");
      when(beetleAmqpConfiguration.getBeetlePolicyUpdatesRoutingKey())
          .thenReturn("beetle.policy.update");

      return beetleAmqpConfiguration;
    }
  }
}
