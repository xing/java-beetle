package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public interface DefaultMessageHandler {

    void process(Envelope envelope, AMQP.BasicProperties properties, byte[] body);
}
