package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.util.concurrent.Callable;

public interface DefaultMessageHandler {

    Callable<HandlerResponse> process(Channel channel, Envelope envelope, AMQP.BasicProperties properties, byte[] body);
}
