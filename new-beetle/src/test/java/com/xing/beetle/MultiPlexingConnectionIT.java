package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.xing.beetle.amqp.MultiPlexingConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MultiPlexingConnectionIT {

    private static final String QUEUE = "test-queue";
    private static final int NUMBER_OF_MESSAGES = 10;

    @Container
    RabbitMQContainer container = new RabbitMQContainer();

    @Test
    void simpleGet() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(container.getContainerIpAddress());
        factory.setPort(container.getAmqpPort());
        Channel channel = new MultiPlexingConnection(factory.newConnection()).createChannel();

        channel.queueDeclare(QUEUE, false, false, true, null);
        for (byte i = 0; i < NUMBER_OF_MESSAGES; i++) {
            channel.basicPublish("", QUEUE, null, new byte[]{i});
        }
        int messageCount = 0;
        GetResponse msg;
        while ((msg = channel.basicGet(QUEUE, true)) != null) {
            messageCount++;
        }
        Assertions.assertEquals(NUMBER_OF_MESSAGES, messageCount);
    }
}
