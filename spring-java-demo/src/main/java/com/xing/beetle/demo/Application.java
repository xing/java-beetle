package com.xing.beetle.demo;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.UUID;

@SpringBootApplication
public class Application {

  private static final String queue = "javaQueue";

  public static void main(String[] args) throws InterruptedException {
    ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
    AmqpTemplate template = context.getBean(AmqpTemplate.class);
    MessageProperties props = new MessageProperties();
    props.setMessageId(UUID.randomUUID().toString());
    Message message = new Message(new byte[0], props);

    for (int i = 0; i < 40; i++) {
      template.send(queue, message);
    }

    for (int i = 0; i < 4; i++) {
      template.convertAndSend(queue, "hello world");
    }

    Thread.sleep(2000);
    context.close();
  }

  @RabbitListener(
      bindings =
          @QueueBinding(
              value = @Queue(value = queue, durable = "false"),
              exchange = @Exchange(value = "auto.exch", ignoreDeclarationExceptions = "true"),
              key = "orderRoutingKey"),
      ackMode = "AUTO")
  public void processOrder(Message message) {
    System.out.println(message);
  }
}
