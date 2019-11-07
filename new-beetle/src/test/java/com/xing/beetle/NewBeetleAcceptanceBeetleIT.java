package com.xing.beetle;

import com.github.dockerjava.api.model.PortBinding;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.amqp.BeetleConnection;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.testcontainers.ContainerLifecycle;
import com.xing.beetle.testcontainers.Containers;
import com.xing.beetle.util.RetryExecutor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
class NewBeetleAcceptanceBeetleIT extends BaseBeetleIT {

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

    @ParameterizedTest(name = "Brokers={0}")
    @ValueSource(ints = {1, 2})
    @ExtendWith(ContainerLifecycle.class)
    void checkWith(@Containers RabbitMQContainer[] containers) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        Stream<Connection> connections = createConnections(factory, containers);
        BeetleConnection beetleConnection = new BeetleConnection(connections.collect(Collectors.toList()));
        Channel channel = beetleConnection.createChannel();
        channel.queueDeclare(QUEUE, false, false, false, null);
        channel.basicPublish("", QUEUE, REDUNDANT.apply(1), "test1".getBytes());
        List<Delivery> messages = new ArrayList<>();
        channel.basicConsume(QUEUE, false, (tag, msg) -> messages.add(msg), System.out::println);

        Thread.sleep(500);
        assertEquals(1 * containers.length, messages.size());
        channel.basicPublish("", QUEUE, REDUNDANT.apply(2), "test2".getBytes());
        Thread.sleep(500);
        assertEquals(2 * containers.length, messages.size());
    }

//    @Test
//    void shouldNotSentMessagesAtStartup() throws Exception {
//        Channel channel = createChannel(new RabbitMQContainer[0], true, -1);
//        assertThrows(IOException.class,
//                () -> channel.basicPublish("", QUEUE, REDUNDANT.get(), "test1".getBytes()));
//    }
}
