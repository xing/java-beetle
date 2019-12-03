package com.xing.beetle.spring;

import com.xing.beetle.BeetleHeader;
import com.xing.beetle.redis.RedisDedupStoreAutoConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
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

  @Autowired private MyService service;

  static {
    List<GenericContainer> rabbitBrokers =
        IntStream.range(0, 2)
            .mapToObj(i -> new GenericContainer("rabbitmq:3.5.3").withExposedPorts(5672))
            .collect(Collectors.toList());
    rabbitBrokers.forEach(GenericContainer::start);

    List<String> addresses =
        rabbitBrokers.stream()
            .map(rabbit -> rabbit.getContainerIpAddress() + ":" + rabbit.getFirstMappedPort())
            .collect(Collectors.toList());

    System.setProperty("spring.rabbitmq.addresses", String.join(",", addresses));
  }

  private void sendRedundantMessages(String routingKey, int redundancy, String messageId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, redundancy);
    props.setMessageId(messageId);
    Message message = new Message("foo".getBytes(), props);
    rabbitTemplate.send("auto.exch", routingKey, message);
  }

  @Test
  public void send2RedundantMessagesShouldReceive1() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessages("test.succeed", 2, messageId);
    System.out.println("1: " + messageId);

    String messageId2 = UUID.randomUUID().toString();
    sendRedundantMessages("test.succeed", 2, messageId2);
    System.out.println("2: " + messageId2);

    waitForMessageDelivery();

    assertEquals(1, result.stream().filter(s -> s.equals(messageId)).count());
    assertEquals(1, result.stream().filter(s -> s.equals(messageId2)).count());
  }

  public void waitForMessageDelivery() {
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void send2RedundantMessagesFirstThrowExceptionThenHandle() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessages("test.withErrThenSucceed", 2, messageId);
    waitForMessageDelivery();
    assertEquals(1, result.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void send2RedundantMessagesHandlerThrowsExceptionExceedsExceptionLimit()
      throws InterruptedException {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessages("test.withErr", 2, messageId);
    waitForMessageDelivery();
    // exception limit is 3
    assertEquals(3, result.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void send2RedundantMessagesHandlerTimesOutExceedsExceptionLimit() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessages("test.withTimeout", 2, messageId);
    waitForMessageDelivery();
    // exception limit is 3
    assertEquals(3, result.stream().filter(s -> s.equals(messageId)).count());
  }

  @Test
  public void send2RedundantMessagesHandlerFirstTimesOutThenSucceeds() {
    String messageId = UUID.randomUUID().toString();
    sendRedundantMessages("test.withTimeoutThenSucceed", 2, messageId);
    waitForMessageDelivery();
    assertEquals(1, result.stream().filter(s -> s.equals(messageId)).count());
  }

  private static CopyOnWriteArrayList<String> result = new CopyOnWriteArrayList<>();

  public static class MyService {

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueueSucceed", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "test.succeed"))
    public void handle(Message message) {
      System.out.println("handle received " + message.getMessageProperties().getMessageId());
      result.add(message.getMessageProperties().getMessageId());
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueueSucceed2", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "test.succeed"))
    public void handle2(Message message) {
      System.out.println("handle2 received " + message.getMessageProperties().getMessageId());
      result.add(message.getMessageProperties().getMessageId());
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueueWithErrThenSucceed", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "test.withErrThenSucceed"))
    public void handleWithErrorThenSucceed(Message message) {
      if (message.getMessageProperties().isRedelivered()) {
        result.add(message.getMessageProperties().getMessageId());
      } else {
        throw new IllegalStateException("message handling failed!");
      }
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueueWithErr", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "test.withErr"))
    public void handleWithError(Message message) {
      result.add(message.getMessageProperties().getMessageId());
      throw new IllegalStateException("message handling failed!");
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueueWithTimeout", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "test.withTimeout"))
    public void handleWithTimeout(Message message) throws InterruptedException {
      result.add(message.getMessageProperties().getMessageId());
      Thread.sleep(1100);
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueueWithTimeoutThenSucceed", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "test.withTimeoutThenSucceed"))
    public void handleWithTimeoutThenSucceed(Message message) throws InterruptedException {
      if (message.getMessageProperties().isRedelivered()) {
        result.add(message.getMessageProperties().getMessageId());
      } else {
        Thread.sleep(2000);
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
    public MyService myService() {
      return new MyService();
    }
  }
}
