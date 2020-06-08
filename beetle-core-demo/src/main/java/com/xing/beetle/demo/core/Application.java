package com.xing.beetle.demo.core;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.BeetleHeader;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.dedup.spi.Deduplicator;
import com.xing.beetle.dedup.spi.KeyValueStoreBasedDeduplicator;
import redis.RedisDedupStore;

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
    beetleAmqpConfiguration.setBeetleRedisServer("localhost:6379");

    RedisDedupStore store = new RedisDedupStore(beetleAmqpConfiguration);
    Deduplicator keyValueStoreBasedDeduplicator =
        new KeyValueStoreBasedDeduplicator(store, beetleAmqpConfiguration);
    BeetleConnectionFactory factory =
        new BeetleConnectionFactory(beetleAmqpConfiguration, keyValueStoreBasedDeduplicator);
    factory.setInvertRequeueParameter(false);

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

    // result  must be 1.
    System.out.println(messages.size());

    connection.close();
  }
}
