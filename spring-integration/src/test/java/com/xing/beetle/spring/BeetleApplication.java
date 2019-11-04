package com.xing.beetle.spring;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import com.xing.beetle.amqp.BeetleConnectionFactory;

@SpringBootApplication
@EnableRabbit
public class BeetleApplication {

  public static void main(String[] args) throws Exception {
    ConfigurableApplicationContext context = SpringApplication.run(BeetleApplication.class, args);
    AmqpTemplate template = context.getBean(AmqpTemplate.class);
    template.convertAndSend("myQueue", "hello world");
    Thread.sleep(1000);
    context.close();
  }

  @Bean
  public ConnectionFactory connectionFactory() {
    BeetleConnectionFactory factory = new BeetleConnectionFactory();
    return new CachingConnectionFactory(factory);
  }

  @RabbitListener(bindings = @QueueBinding(
      value = @Queue(value = "myQueue", autoDelete = "true", durable = "true"),
      exchange = @Exchange(value = "auto.exch", autoDelete = "true",
          ignoreDeclarationExceptions = "true"),
      key = "orderRoutingKey"))
  public void processOrder(String body) {
    System.out.println(body);
  }
}
