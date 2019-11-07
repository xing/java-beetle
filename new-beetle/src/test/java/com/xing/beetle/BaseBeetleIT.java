package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.util.RetryExecutor;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Testcontainers
class BaseBeetleIT {

    static final String QUEUE = "testQueue";

    static final Supplier<AMQP.BasicProperties> REDUNDANT = () -> new AMQP.BasicProperties.Builder()
            .headers(Map.of(BeetleHeader.PUBLISH_REDUNDANCY, 2)).build();

    static Channel createConnection(RabbitMQContainer[] containers, boolean lazy, long requeueAtEndDelayInMillis) throws Exception {
        BeetleConnectionFactory factory = new BeetleConnectionFactory();
        factory.setRequeueAtEndDelayInMillis(requeueAtEndDelayInMillis);
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
}
