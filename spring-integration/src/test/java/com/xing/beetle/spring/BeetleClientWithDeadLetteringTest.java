package com.xing.beetle.spring;

import com.xing.beetle.BeetleHeader;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.redis.RedisDedupStoreAutoConfiguration;
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
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

/**
 * Full blown beetle client test with spring integration (RabbitListener) and deduplication (with
 * Redis) where dead lettering enabled.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ExtendWith(MockitoExtension.class)
@SpringBootTest
@DirtiesContext
public class BeetleClientWithDeadLetteringTest {

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

    System.setProperty("test.deadLetterEnabled", "true");
  }

  @Test
  public void throwExceptionExceedExceptionLimitWithDeadLettering() throws InterruptedException {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithErrorDL", 2, messageId);
    waitForMessageDelivery(8000);
    // exception limit is 3
    service.assertCounts(messageId, 3, 1, 0, 10000);
  }

  @Test
  public void timeoutExceedExceptionLimitWithDeadLettering() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithTimeoutDL", 2, messageId);
    waitForMessageDelivery(8000);
    // exception limit is 3
    service.assertCounts(messageId, 3, 1, 0, 10000);

    // make sure that queue for policy is declared and working
    assertFalse(service.queuePolicyMessages.isEmpty());
  }

  private void sendRedundantMessage(String routingKey, int redundancy, String messageId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, redundancy);
    props.setMessageId(messageId);
    Message message = new Message("foo".getBytes(), props);
    rabbitTemplate.send("", routingKey, message);
  }

  public void waitForMessageDelivery(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static class MessageHandlingService extends RecordingMessageHandler {

    @RabbitListener(queues = "QueueWithErrorDL")
    public void handleWithErrorDeadLettered(Message message) {
      if (message.getMessageProperties().getHeader("x-death") != null) {
        log.log(
            System.Logger.Level.DEBUG,
            "deadlettered: %s\n",
            message.getMessageProperties().getMessageId());
        deadLettered.add(message.getMessageProperties().getMessageId());
      }
      if (message.getMessageProperties().isRedelivered()) {
        log.log(
            System.Logger.Level.DEBUG,
            "redelivered: %s\n",
            message.getMessageProperties().getMessageId());
        redelivered.add(message.getMessageProperties().getMessageId());
      }
      result.add(message.getMessageProperties().getMessageId());
      throw new IllegalStateException(
          "message handling failed for " + message.getMessageProperties().getMessageId());
    }

    @RabbitListener(queues = "QueueWithTimeoutDL")
    public void handleWithTimeoutDeadLettered(Message message) throws InterruptedException {
      if (message.getMessageProperties().getHeader("x-death") != null) {
        log.log(
            System.Logger.Level.DEBUG,
            "deadlettered: %s\n",
            message.getMessageProperties().getMessageId());
        deadLettered.add(message.getMessageProperties().getMessageId());
      }
      if (message.getMessageProperties().isRedelivered()) {
        log.log(
            System.Logger.Level.DEBUG,
            "redelivered: %s\n",
            message.getMessageProperties().getMessageId());
        redelivered.add(message.getMessageProperties().getMessageId());
      }
      result.add(message.getMessageProperties().getMessageId());
      Thread.sleep(1100);
    }

    @RabbitListener(queues = "beetle-policy-updates")
    public void receiveQueuePolicyMessages(Message message) {
      queuePolicyMessages.add(message.getMessageProperties().getMessageId());
    }
  }

  @Configuration
  @ConditionalOnProperty(value = "test.deadLetterEnabled", havingValue = "true")
  @EnableRabbit
  @EnableTransactionManagement
  @SpringBootConfiguration
  @EnableAutoConfiguration
  public static class EnableRabbitConfig {

    @Bean
    @ConditionalOnProperty(value = "test.deadLetterEnabled", havingValue = "true")
    public MessageHandlingService handlingService() {
      return new MessageHandlingService();
    }

    @Bean
    public org.springframework.amqp.core.Queue queueWithErrorDeadLettered() {
      return new org.springframework.amqp.core.Queue("QueueWithErrorDL", true, true, false, null);
    }

    @Bean
    public org.springframework.amqp.core.Queue queueWithTimeoutDeadLettered() {
      return new org.springframework.amqp.core.Queue("QueueWithTimeoutDL", true, true, false, null);
    }

    @Bean
    @Primary
    @ConditionalOnProperty(value = "test.deadLetterEnabled", havingValue = "true")
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
      when(beetleAmqpConfiguration.getDeadLetteringMsgTtlMs()).thenReturn(10);
      when(beetleAmqpConfiguration.isDeadLetteringEnabled()).thenReturn(true);
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
