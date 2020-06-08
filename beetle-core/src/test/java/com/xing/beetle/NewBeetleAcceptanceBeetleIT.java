package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.xing.beetle.Assertions.assertEventualLength;
import static org.mockito.Mockito.when;

@Testcontainers
class NewBeetleAcceptanceBeetleIT extends BaseBeetleIT {

  @ParameterizedTest(name = "Brokers={0}")
  @ValueSource(ints = {1, 2})
  void testRedundantPublishWithoutDeduplication(int containers) throws Exception {
    BeetleConnectionFactory factory =
        new BeetleConnectionFactory(beetleAmqpConfiguration(containers));

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    String queue = String.format("nodedup-%d", containers);

    channel.queueDeclare(queue, false, false, false, null);
    channel.basicPublish("", queue, REDUNDANT.apply(1), "test1".getBytes());

    List<Delivery> messages = new ArrayList<>();
    channel.basicConsume(queue, true, (tag, msg) -> messages.add(msg), System.out::println);

    assertEventualLength(messages, 1, 500);
    messages.clear();

    channel.basicPublish("", queue, REDUNDANT.apply(2), "test2".getBytes());

    assertEventualLength(messages, Math.min(containers, 2), 500);
  }

  BeetleAmqpConfiguration beetleAmqpConfiguration(int containers) {

    List<String> rabbitAddresses =
        Arrays.stream(rmq)
            .map(rabbit -> rabbit.getContainerIpAddress() + ":" + rabbit.getFirstMappedPort())
            .limit(containers)
            .collect(Collectors.toList());

    BeetleAmqpConfiguration beetleAmqpConfiguration = Mockito.mock(BeetleAmqpConfiguration.class);

    when(beetleAmqpConfiguration.getBeetleServers()).thenReturn(String.join(",", rabbitAddresses));
    when(beetleAmqpConfiguration.getBeetlePolicyExchangeName()).thenReturn("beetle-policies");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesQueueName())
        .thenReturn("beetle-policy-updates");
    when(beetleAmqpConfiguration.getBeetlePolicyUpdatesRoutingKey())
        .thenReturn("beetle.policy.update");

    return beetleAmqpConfiguration;
  }
}
