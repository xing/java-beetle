package com.xing.beetle;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import com.github.dockerjava.api.model.PortBinding;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.amqp.BeetleChannel;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.testcontainers.ContainerLifecycle;
import com.xing.beetle.testcontainers.Containers;
import com.xing.beetle.util.RetryExecutor;

@Testcontainers
class NewBeetleAcceptanceIT {

  @Nested
  class StartupWithUnreachableRabbit {

    private final int port = ThreadLocalRandom.current().nextInt(1024, 65535);

    RabbitMQContainer rabbit =
        new RabbitMQContainer().withExposedPorts(5672).withCreateContainerCmdModifier(
            cmd -> cmd.withPortBindings(PortBinding.parse(port + ":5672")));

    @Test
    @Disabled
    void testLateStartup() throws Exception {
      BeetleConnectionFactory factory = new BeetleConnectionFactory();
      factory.setConnectionEstablishingExecutor(RetryExecutor.ASYNC_EXPONENTIAL);
      factory.setHost("localhost");
      factory.setPort(port);
      Channel channel = factory.newConnection().createChannel();
      channel.queueDeclare(QUEUE, false, false, false, null);
      rabbit.start();
      channel.basicPublish("", QUEUE, null, "test1".getBytes());
    }
  }

  static final String QUEUE = "testQueue";

  static final Supplier<BasicProperties> REDUNDANT = () -> new BasicProperties.Builder()
      .headers(Map.of(BeetleChannel.REDUNDANT_HEADER_KEY, 2)).build();

  static Channel createConnection(RabbitMQContainer[] containers, boolean lazy) throws Exception {
    BeetleConnectionFactory factory = new BeetleConnectionFactory();
    factory.setConnectionEstablishingExecutor(
        lazy ? RetryExecutor.ASYNC_IMMEDIATELY : RetryExecutor.SYNCHRONOUS);
    Address[] addresses = Stream.of(containers).map(RabbitMQContainer::getAmqpUrl)
        .map(s -> s.substring(7)).map(Address::parseAddress).toArray(Address[]::new);
    if (addresses.length > 0) {
      return factory.newConnection(addresses).createChannel();
    } else {
      return factory.newConnection().createChannel();
    }
  }

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  @ExtendWith(ContainerLifecycle.class)
  void checkWith(@Containers RabbitMQContainer[] containers) throws Exception {
    Channel channel = createConnection(containers, false);
    channel.queueDeclare(QUEUE, false, false, false, null);
    channel.basicPublish("", QUEUE, REDUNDANT.get(), "test1".getBytes());
    List<Delivery> messages = new ArrayList<>();
    channel.basicConsume(QUEUE, true, (tag, msg) -> messages.add(msg), System.out::println);

    Thread.sleep(500);
    assertEquals(1 * containers.length, messages.size());
    channel.basicPublish("", QUEUE, REDUNDANT.get(), "test2".getBytes());
    Thread.sleep(500);
    assertEquals(2 * containers.length, messages.size());
  }

  @Test
  void shouldNotSentMessagesAtStartup() throws Exception {
    Channel channel = createConnection(new RabbitMQContainer[0], true);
    assertThrows(IOException.class,
        () -> channel.basicPublish("", QUEUE, REDUNDANT.get(), "test1".getBytes()));
  }
}
