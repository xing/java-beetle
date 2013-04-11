package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.util.concurrent.Callable;

public interface DefaultMessageHandler {

    Callable<HandlerResponse> process(Envelope envelope, AMQP.BasicProperties properties, byte[] body);
}
