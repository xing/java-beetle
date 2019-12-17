package com.xing.beetle.spring;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import com.rabbitmq.client.Channel;
import com.xing.beetle.BeetleHeader;

@SpringBootApplication
@EnableRabbit
public class BeetleRedisApplication {

  private static final String QUEUE = "myQueue1234";

  static MessagePostProcessor redundant(int redundancy) {
    return msg -> {
      msg.getMessageProperties().setHeader(BeetleHeader.PUBLISH_REDUNDANCY, redundancy);
      return msg;
    };
  }

  public static void main(String[] args) throws Exception {
    ConfigurableApplicationContext context =
        SpringApplication.run(BeetleRedisApplication.class, args);
    AmqpTemplate template = context.getBean(RabbitTemplate.class);
    Object message = "hello world";
    template.convertAndSend(QUEUE, message, redundant(2));
    Thread.sleep(100000);
    context.close();
  }

  @RabbitListener(
      ackMode = "AUTO",
      queuesToDeclare = @Queue(name = QUEUE, durable = "true", autoDelete = "false"))
  // arguments = @Argument(name = BeetleHeader.REQUEUE_AT_END_DELAY, value = "PT1S")))
  public void onMessage(Message message, Channel channel) throws Exception {
    //    if (!message.getMessageProperties().isRedelivered()) {
    //      Thread.sleep(30000);
    //    }
    System.out.println(message);
  }
}
