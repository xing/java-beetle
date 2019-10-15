package com.xing.beetle;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.github.dockerjava.api.model.PortBinding;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;

@Testcontainers
class RabbitRecoverableIT {

  private static final int PORT = ThreadLocalRandom.current().nextInt(1024, 65535);

  @Container
  RabbitMQContainer rabbit =
      new RabbitMQContainer()
          .withExposedPorts(5672)
          .withCreateContainerCmdModifier(
              cmd -> cmd.withPortBindings(PortBinding.parse(PORT + ":5672")));

  Channel channel;

  @BeforeEach
  void init() throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rabbit.getContainerIpAddress());
    factory.setPort(rabbit.getAmqpPort());
    factory.setNetworkRecoveryInterval(500);
    channel = factory.newConnection().createChannel();
  }

  void restart(GenericContainer<?> container) {
    container.stop();
    container.start();
  }

  @Test
  void simpleRecovery() throws Exception {
    String queue = "testQueue";
    channel.queueDeclare(queue, false, false, false, null);
    channel.basicPublish("", queue, null, "test1".getBytes());
    List<Delivery> messages = new ArrayList<>();
    channel.basicConsume(queue, true, (tag, msg) -> messages.add(msg), System.out::println);

    restart(rabbit);

    assertEquals(1, messages.size());
    Thread.sleep(100);
    channel.basicPublish("", queue, null, "test2".getBytes());
    Thread.sleep(100);
    assertEquals(2, messages.size());
  }
}
