package com.xing.beetle.demo.core;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.BeetleHeader;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Sample application that publishes and consumes messages using Java Beetle Client. (without Spring
 * or any other framework)
 */
public class Application {
  public static void main(String[] args)
      throws IOException, TimeoutException, InterruptedException {

    BeetleAmqpConfiguration beetleAmqpConfiguration = new BeetleAmqpConfiguration();
    beetleAmqpConfiguration.setBeetleServers("localhost:5673,localhost:5672");

    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    String queue = "queue-beetle-core-demo";
    channel.queueDeclare(queue, false, false, false, null);

    AMQP.BasicProperties props = new AMQP.BasicProperties();
    Map<String, Object> headers = new HashMap<>();
    headers.put(BeetleHeader.PUBLISH_REDUNDANCY, 2);
    props = props.builder().headers(headers).build();

    channel.basicPublish("", queue, props, "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    channel.basicConsume(queue, true, (tag, msg) -> messages.add(msg), System.out::println);

    Thread.sleep(2000);

    // For the time being, the result must be 2, since there is no  deduplication involved
    // We are working on deduplication without need for @RabbitListener
    System.out.println(messages.size());

    connection.close();
  }
}
