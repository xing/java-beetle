package com.xing.beetle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.testcontainers.ContainerLifecycle;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class NewBeetleAcceptanceBeetleIT extends BaseBeetleIT {

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = { 1, 2 })
  @ExtendWith(ContainerLifecycle.class)
  void testRedundantPublishWithoutDeduplication(int containers) throws Exception {
    BeetleConnectionFactory factory = new BeetleConnectionFactory(beetleAmqpConfiguration());
    Connection connection = createConnection(factory, containers);
    Channel channel = connection.createChannel();
    String queue = String.format("nodedup-%d", containers);

    channel.queueDeclare(queue, false, false, false, null);
    channel.basicPublish("", queue, REDUNDANT.apply(1), "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    channel.basicConsume(queue, true, (tag, msg) -> messages.add(msg), System.out::println);

    Thread.sleep(500);
    assertEquals(1, messages.size());
    messages.clear();
    channel.basicPublish("", queue, REDUNDANT.apply(2), "test2".getBytes());
    Thread.sleep(500);
    assertEquals(Math.min(containers, 2), messages.size());
  }

  BeetleAmqpConfiguration beetleAmqpConfiguration() {
    BeetleAmqpConfiguration beetleAmqpConfiguration = Mockito.mock(BeetleAmqpConfiguration.class);

    when(beetleAmqpConfiguration.getBeetlePolicyExchangeName()).thenReturn("beetle-policies");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesQueueName()).thenReturn("beetle-policy-updates");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesRoutingKey()).thenReturn("beetle.policy.update");

    when(beetleAmqpConfiguration.getBeetleServers()).thenReturn("");

    return beetleAmqpConfiguration;
  }
}
