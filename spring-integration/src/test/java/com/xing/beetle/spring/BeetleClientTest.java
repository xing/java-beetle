package com.xing.beetle.spring;

import com.xing.beetle.BeetleHeader;
import com.xing.beetle.redis.RedisDedupStoreAutoConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Full blown beetle client test with spring integration (RabbitListener) and deduplication (with
 * Redis).
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class BeetleClientTest {

  @Autowired private RabbitTemplate rabbitTemplate;

  @Autowired private MessageHandlingService service;

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

    System.setProperty("spring.rabbitmq.addresses", String.join(",", rabbitAddresses));
    System.setProperty(
        "beetle.redis.redis_server",
        String.join(
            ":", new String[] {redis.getContainerIpAddress(), redis.getFirstMappedPort() + ""}));
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

    waitForMessageDelivery(2000);

    assertEquals(1, result.stream().filter(s -> s.equals(messageId)).count());
    assertEquals(1, result.stream().filter(s -> s.equals(messageId2)).count());
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
    waitForMessageDelivery(16000);
    // exception limit is 3
    assertEquals(1, redelivered.stream().filter(s -> s.equals(messageId)).count());
    assertEquals(3, result.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void throwExceptionExceedExceptionLimitWithDeadLettering() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithErrorDL", 2, messageId);
    waitForMessageDelivery(16000);
    // exception limit is 3
    assertEquals(3, result.stream().filter(s -> s.equals(messageId)).count());
    assertEquals(1, deadLettered.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void timeoutExceedExceptionLimit() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithTimeout", 2, messageId);
    waitForMessageDelivery(16000);
    // exception limit is 3
    assertEquals(3, result.stream().filter(s -> s.equals(messageId)).count());
    assertEquals(1, redelivered.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void timeoutExceedExceptionLimitWithDeadLettering() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithTimeoutDL", 2, messageId);
    waitForMessageDelivery(16000);
    // exception limit is 3
    assertEquals(3, result.stream().filter(s -> s.equals(messageId)).count());
    assertEquals(1, deadLettered.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void firstTimeoutThenSucceed() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithTimeoutThenSucceed", 2, messageId);
    waitForMessageDelivery(4000);
    assertEquals(1, result.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void firstThrowExceptionThenHandle() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessage("QueueWithErrorThenSucceed", 2, messageId);
    waitForMessageDelivery(4000);
    assertEquals(1, result.stream().filter(s -> s.equals(messageId)).count());
  }

  private static final CopyOnWriteArrayList<String> result = new CopyOnWriteArrayList<>();
  private static final CopyOnWriteArrayList<String> redelivered = new CopyOnWriteArrayList<>();
  private static final CopyOnWriteArrayList<String> deadLettered = new CopyOnWriteArrayList<>();

  public static class MessageHandlingService {

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
      if (message.getMessageProperties().isRedelivered()) {
        redelivered.add(message.getMessageProperties().getMessageId());
      }
      result.add(message.getMessageProperties().getMessageId());
      throw new IllegalStateException(
          "message handling failed for " + message.getMessageProperties().getMessageId());
    }

    @RabbitListener(queues = "QueueWithErrorDL")
    public void handleWithErrorDeadLettered(Message message) {
      if (message.getMessageProperties().getHeader("x-death") != null) {
        deadLettered.add(message.getMessageProperties().getMessageId());
      }
      result.add(message.getMessageProperties().getMessageId());
      throw new IllegalStateException(
          "message handling failed for " + message.getMessageProperties().getMessageId());
    }

    @RabbitListener(queues = "QueueWithTimeout")
    public void handleWithTimeout(Message message) throws InterruptedException {
      if (message.getMessageProperties().isRedelivered()) {
        redelivered.add(message.getMessageProperties().getMessageId());
      }
      result.add(message.getMessageProperties().getMessageId());
      Thread.sleep(1100);
    }

    @RabbitListener(queues = "QueueWithTimeoutDL")
    public void handleWithTimeoutDeadLettered(Message message) throws InterruptedException {
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
  }

  @Configuration
  @EnableRabbit
  @EnableTransactionManagement
  @ComponentScan(
      basePackageClasses = {
        RedisDedupStoreAutoConfiguration.class,
        BeetleAutoConfiguration.class,
        BeetleListenerInterceptor.class,
        CustomizableConnectionFactoryBean.class
      })
  public static class EnableRabbitConfig {

    @Bean
    public MessageHandlingService myService() {
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
    public org.springframework.amqp.core.Queue queueWithErrorDeadLettered() {
      Map<String, Object> arguments = new HashMap<>();
      arguments.put(BeetleHeader.REQUEUE_AT_END_DELAY, "PT1S");
      return new org.springframework.amqp.core.Queue(
          "QueueWithErrorDL", true, true, true, arguments);
    }

    @Bean
    public org.springframework.amqp.core.Queue queueWithTimeoutDeadLettered() {
      Map<String, Object> arguments = new HashMap<>();
      arguments.put(BeetleHeader.REQUEUE_AT_END_DELAY, "PT1S");
      return new org.springframework.amqp.core.Queue(
          "QueueWithTimeoutDL", true, true, true, arguments);
    }
  }
}
