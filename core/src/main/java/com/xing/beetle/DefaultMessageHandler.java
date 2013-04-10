package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public interface DefaultMessageHandler {

    FutureHandlerResponse process(Envelope envelope, AMQP.BasicProperties properties, byte[] body);
}
