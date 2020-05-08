package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.testcontainers.ContainerLifecycle;
import com.xing.beetle.testcontainers.Containers;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@Testcontainers
class NewBeetleAcceptanceBeetleIT extends BaseBeetleIT {

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  @ExtendWith(ContainerLifecycle.class)
  void testRedundantPublishWithoutDeduplication(@Containers RabbitMQContainer[] containers)
      throws Exception {
    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration());
    Connection connection = createConnection(factory, containers);
    Channel channel = connection.createChannel();
    channel.queueDeclare(QUEUE, false, false, false, null);
    channel.basicPublish("", QUEUE, REDUNDANT.apply(1), "test1".getBytes());
    List<Delivery> messages = new ArrayList<>();
    channel.basicConsume(QUEUE, true, (tag, msg) -> messages.add(msg), System.out::println);

    Thread.sleep(500);
    assertEquals(1, messages.size());
    messages.clear();
    channel.basicPublish("", QUEUE, REDUNDANT.apply(2), "test2".getBytes());
    Thread.sleep(500);
    assertEquals(Math.min(containers.length, 2), messages.size());
  }

  BeetleAmqpConfiguration beetleAmqpConfiguration() {
    BeetleAmqpConfiguration beetleAmqpConfiguration = Mockito.mock(BeetleAmqpConfiguration.class);

    when(beetleAmqpConfiguration.getBeetlePolicyExchangeName()).thenReturn("beetle-policies");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesQueueName())
        .thenReturn("beetle-policy-updates");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesRoutingKey())
        .thenReturn("beetle.policy.update");

    when(beetleAmqpConfiguration.getBeetleServers()).thenReturn("");

    return beetleAmqpConfiguration;
  }
}
