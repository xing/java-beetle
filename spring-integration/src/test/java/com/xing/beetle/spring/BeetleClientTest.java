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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Full blown beetle client test with spring integration (RabbitListener) and deduplication (with
 * Redis).
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class BeetleClientTest {

  @Autowired private RabbitTemplate rabbitTemplate;

  @Autowired private MyService service;

  @Test
  public void send2RedundantMessagesShouldReceive1() throws InterruptedException {
    result.clear();
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, 2);
    props.setMessageId(UUID.randomUUID().toString());
    Message message = new Message("foo".getBytes(), props);
    for (int i = 0; i < 2; i++) {
      rabbitTemplate.send("", "Que", message);
    }
    Thread.sleep(1000);
    assertEquals(1, result.size());
  }

  @Test
  public void send2RedundantMessagesFirstThrowExceptionThenHandle() throws InterruptedException {
    result.clear();
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, 2);
    props.setMessageId(UUID.randomUUID().toString());
    Message message = new Message("foo".getBytes(), props);
    for (int i = 0; i < 2; i++) {
      rabbitTemplate.send("", "QueWithErrThenSucceed", message);
    }
    Thread.sleep(2000);
    assertEquals(1, result.size());
  }

  @Test
  public void send2RedundantMessagesHandlerThrowsExceptionExceedsExceptionLimit()
      throws InterruptedException {
    result.clear();
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, 2);
    props.setMessageId(UUID.randomUUID().toString());
    Message message = new Message("foo".getBytes(), props);
    for (int i = 0; i < 2; i++) {
      rabbitTemplate.send("", "QueWithErr", message);
    }
    Thread.sleep(3000);
    // exception limit is 3
    assertEquals(3, result.size());
  }

  @Test
  public void send2RedundantMessagesHandlerTimesOutExceedsExceptionLimit()
      throws InterruptedException {
    result.clear();
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, 2);
    props.setMessageId(UUID.randomUUID().toString());
    Message message = new Message("foo".getBytes(), props);
    for (int i = 0; i < 2; i++) {
      rabbitTemplate.send("", "QueWithTimeout", message);
    }
    Thread.sleep(6000);
    // exception limit is 3
    assertEquals(3, result.size());
  }

  @Test
  public void send2RedundantMessagesHandlerFirstTimesOutThenSucceeds() throws InterruptedException {
    result.clear();
    MessageProperties props = new MessageProperties();
    props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, 2);
    props.setMessageId(UUID.randomUUID().toString());
    Message message = new Message("foo".getBytes(), props);
    for (int i = 0; i < 2; i++) {
      rabbitTemplate.send("", "QueWithTimeoutThenSucceed", message);
    }
    Thread.sleep(3000);
    assertEquals(1, result.size());
  }

  private static List<String> result = new ArrayList<>();

  public static class MyService {

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "Que", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "auto.rk"))
    public void handle(String foo) {
      result.add(foo.toUpperCase());
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueWithErrThenSucceed", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "auto.rk"))
    public void handleWithErrorThenSucceed(Message message) {
      if (message.getMessageProperties().isRedelivered()) {
        result.add(message.getBody().toString().toUpperCase());
      } else {
        throw new IllegalStateException("message handling failed!");
      }
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueWithErr", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "auto.rk"))
    public void handleWithError(Message message) {
      result.add(message.getBody().toString().toUpperCase());
      throw new IllegalStateException("message handling failed!");
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueWithTimeout", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "auto.rk"))
    public void handleWithTimeout(Message message) throws InterruptedException {
      result.add(message.getBody().toString().toUpperCase());
      Thread.sleep(1100);
    }

    @RabbitListener(
        bindings =
            @QueueBinding(
                value = @Queue(value = "QueWithTimeoutThenSucceed", autoDelete = "true"),
                exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
                key = "auto.rk"))
    public void handleWithTimeoutThenSucceed(Message message) throws InterruptedException {
      if (message.getMessageProperties().isRedelivered()) {
        result.add(message.getBody().toString().toUpperCase());
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
