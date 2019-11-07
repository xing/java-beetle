package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.xing.beetle.amqp.MultiPlexingConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Testcontainers
public class MultiPlexingConnectionIT {

    enum AckStrategy {
        AUTO {
            @Override
            void ack(Channel channel, long deliveryTag) throws IOException {
            }
        }, SINGLE {
            @Override
            void ack(Channel channel, long deliveryTag) throws IOException {
                channel.basicAck(deliveryTag, false);
            }
        }, MULTIPLE {
            @Override
            void ack(Channel channel, long deliveryTag) throws IOException {
                if (deliveryTag == NUMBER_OF_MESSAGES) {
                    channel.basicAck(deliveryTag, true);
                }
            }
        };

        abstract void ack(Channel channel, long deliveryTag) throws IOException;

        boolean isAuto() {
            return this == AUTO;
        }
    }

    enum ReadMode {
        GET {
            @Override
            int read(Channel channel, AckStrategy strategy) throws Exception {
                int messageCount = 0;
                GetResponse msg;
                while ((msg = channel.basicGet(QUEUE, strategy.isAuto())) != null) {
                    messageCount++;
                    strategy.ack(channel, msg.getEnvelope().getDeliveryTag());
                }
                return messageCount;
            }
        }, CONSUME {
            @Override
            int read(Channel channel, AckStrategy strategy) throws Exception {
                AtomicInteger messageCount = new AtomicInteger();
                channel.basicConsume(QUEUE, strategy.isAuto(), (tag, msg) -> {
                    messageCount.incrementAndGet();
                    strategy.ack(channel, msg.getEnvelope().getDeliveryTag());
                }, System.err::println);
                Thread.sleep(1000);
                return messageCount.get();
            }
        };

        abstract int read(Channel channel, AckStrategy strategy) throws Exception;
    }

    private static final String QUEUE = "test-queue";
    private static final int NUMBER_OF_MESSAGES = 10;

    @Container
    RabbitMQContainer container = new RabbitMQContainer();

    @ParameterizedTest
    @CsvSource({"GET,AUTO", "GET,SINGLE", "GET,MULTIPLE", "CONSUME,AUTO", "CONSUME,SINGLE", "CONSUME,MULTIPLE"})
    void test(ReadMode mode, AckStrategy strategy) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(container.getContainerIpAddress());
        factory.setPort(container.getAmqpPort());
        MultiPlexingConnection connection = new MultiPlexingConnection(factory.newConnection());
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE, false, false, false, null);
        for (byte i = 0; i < NUMBER_OF_MESSAGES; i++) {
            channel.basicPublish("", QUEUE, null, new byte[]{i});
        }

        int messageCount = mode.read(channel, strategy);
        Assertions.assertEquals(NUMBER_OF_MESSAGES, messageCount);

        channel.close();
        channel = connection.createChannel();
        Assertions.assertEquals(0, channel.messageCount(QUEUE));
    }
}
