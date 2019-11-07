package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.xing.beetle.testcontainers.ContainerLifecycle;
import com.xing.beetle.testcontainers.Containers;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Testcontainers
class BeetleChannelIT extends BaseBeetleIT {

    @ParameterizedTest(name = "BeetleChannel={0}")
    @ValueSource(ints = {1, 2, 3})
    @ExtendWith(ContainerLifecycle.class)
    void testBasicConsumeAutoAck(@Containers RabbitMQContainer[] containers) throws Exception {
        Channel channel = createConnection(containers, false, -1);
        channel.queueDeclare(QUEUE, false, false, false, null);
        channel.basicPublish("", QUEUE, REDUNDANT.get(), "test1".getBytes());
        List<Delivery> messages = new ArrayList<>();
        channel.basicConsume(QUEUE, true, (tag, msg) -> messages.add(msg), System.out::println);
        Thread.sleep(500);
        assertEquals(getExpectedNumMessages(containers), messages.size());
    }

    private int getExpectedNumMessages(@Containers RabbitMQContainer[] containers) {
        return Math.min((int) REDUNDANT.get().getHeaders().get(BeetleHeader.PUBLISH_REDUNDANCY), containers.length);
    }

    @ParameterizedTest(name = "BeetleChannel={0}")
    @ValueSource(ints = {1, 2, 3})
    @ExtendWith(ContainerLifecycle.class)
    void testBasicAck(@Containers RabbitMQContainer[] containers) throws Exception {
        Channel channel = createConnection(containers, false, -1);
        channel.queueDeclare(QUEUE, false, false, false, null);
        channel.basicPublish("", QUEUE, REDUNDANT.get(), "test1".getBytes());
        List<Delivery> messages = new ArrayList<>();
        channel.basicConsume(QUEUE, false, (tag, msg) -> {
            channel.basicAck(msg.getEnvelope().getDeliveryTag(), false);
            messages.add(msg);
        }, System.out::println);
        Thread.sleep(100);
        assertEquals(getExpectedNumMessages(containers), messages.size());
    }

    @ParameterizedTest(name = "BeetleChannel={0}")
    @ValueSource(ints = {1, 2, 3})
    @ExtendWith(ContainerLifecycle.class)
    void testBasicReject(@Containers RabbitMQContainer[] containers) throws Exception {
        Channel channel = createConnection(containers, false, -1);
        channel.queueDeclare(QUEUE, false, false, false, null);
        channel.basicPublish("", QUEUE, REDUNDANT.get(), "test1".getBytes());
        List<Delivery> messages = new ArrayList<>();
        channel.basicConsume(QUEUE, false, (tag, msg) -> {
            channel.basicReject(msg.getEnvelope().getDeliveryTag(), true);
            messages.add(msg);
        }, System.out::println);

        Thread.sleep(100);
        assertTrue(messages.size() > getExpectedNumMessages(containers));
    }
}
