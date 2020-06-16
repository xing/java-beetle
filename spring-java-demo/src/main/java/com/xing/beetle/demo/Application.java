package com.xing.beetle.demo;

import com.xing.beetle.BeetleHeader;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class Application {

  private static final String queueRabbitListener = "javaQueueRabbitListener";
  private static final String queueRabbitTemplate = "javaQueueRabbitTemplate";
  private static AtomicInteger countRabbitListener = new AtomicInteger(0);

  public static void main(String[] args) throws InterruptedException {
    ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
    AmqpTemplate template = context.getBean(AmqpTemplate.class);

    RabbitAdmin rabbitAdmin = context.getBean(RabbitAdmin.class);
    rabbitAdmin.declareQueue(new org.springframework.amqp.core.Queue(queueRabbitTemplate));

    int messageCount = 4;
    for (int i = 0; i < messageCount; i++) {
      MessageProperties props = new MessageProperties();
      props.setMessageId(UUID.randomUUID().toString());
      props.setHeader(BeetleHeader.PUBLISH_REDUNDANCY, 2);
      Message message = new Message(new byte[0], props);
      template.send(queueRabbitListener, message);
      template.send(queueRabbitTemplate, message);
    }

    Message receivedByTemplate = null;
    int countRabbitTemplate = 0;
    while ((receivedByTemplate = template.receive(queueRabbitTemplate, 2000)) != null) {
      System.out.println(receivedByTemplate);
      countRabbitTemplate++;
    }

    Thread.sleep(2000);

    // When there are multiple brokers

    if (countRabbitListener.get() == messageCount) {
      System.out.println("Deduplication works for @RabbitListener as expected.");
    } else {
      System.out.println("Deduplication is broken for @RabbitListener. Normally it SHOULD work.");
    }

    if (countRabbitTemplate == messageCount) {
      System.out.println(
          "Probably only there is only one broker. Deduplication does NOT work for RabbitTemplate");
    } else {
      System.out.println("Deduplication does NOT work for RabbitTemplate as expected.");
    }

    context.close();
  }

  @RabbitListener(
      bindings =
          @QueueBinding(
              value = @Queue(value = queueRabbitListener, durable = "false"),
              exchange = @Exchange(value = "auto.exch", ignoreDeclarationExceptions = "true"),
              key = "orderRoutingKey"),
      ackMode = "AUTO")
  public void processOrder(Message message) {
    System.out.println(message);
    countRabbitListener.incrementAndGet();
  }
}
