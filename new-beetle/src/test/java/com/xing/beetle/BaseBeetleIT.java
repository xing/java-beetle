package com.xing.beetle;

import com.rabbitmq.client.*;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.util.ExceptionSupport;
import com.xing.beetle.util.RetryExecutor;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Testcontainers
class BaseBeetleIT {

    static final String QUEUE = "testQueue";

    static final IntFunction<AMQP.BasicProperties> REDUNDANT = r -> new AMQP.BasicProperties.Builder()
            .headers(Map.of(BeetleHeader.PUBLISH_REDUNDANCY, r)).build();

//    static Channel createChannel(RabbitMQContainer[] containers, boolean lazy, long requeueAtEndDelayInMillis) throws Exception {
//        BeetleConnectionFactory factory = new BeetleConnectionFactory();
//        factory.setRequeueAtEndDelayInMillis(requeueAtEndDelayInMillis);
//        factory.setConnectionEstablishingExecutor(
//                lazy ? RetryExecutor.ASYNC_IMMEDIATELY : RetryExecutor.SYNCHRONOUS);
//        return createConnections(factory, containers).createChannel();
//    }

    static Stream<Connection> createConnections(ConnectionFactory factory, RabbitMQContainer[] containers) throws Exception {
        return Stream.of(containers).map(rabbitMQContainer -> createConnection(factory, rabbitMQContainer));
    }

    static Connection createConnection(ConnectionFactory factory, RabbitMQContainer container) {
        String amqpUrl = container.getAmqpUrl();
        Address address = Address.parseAddress(amqpUrl.substring(7));
        try {
            return factory.newConnection(new Address[]{address});
        } catch (Exception e) {
            return ExceptionSupport.sneakyThrow(e);
        }
    }
}
