package com.xing.beetle.spring;

import com.xing.beetle.amqp.BeetleConnectionFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableRabbit
public class BeetleApplication {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context = SpringApplication.run(BeetleApplication.class, args);
        AmqpTemplate template = context.getBean(AmqpTemplate.class);
        for (int i = 0; i < 10; i++) {
            template.convertAndSend("myQueue", "hello world");
            template.convertAndSend("myQueue", "goodbye world");
        }
        Thread.sleep(10000);
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
            key = "orderRoutingKey"), ackMode = "MANUAL")
    public void processOrder(Message message) {
        System.out.println(message.getMessageProperties().getDeliveryTag());
    }
}
