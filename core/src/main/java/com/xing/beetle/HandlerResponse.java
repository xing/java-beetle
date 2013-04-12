package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

/**
 *
 */
public class HandlerResponse {

    public HandlerResponse(ResponseCode responseCode, Channel channel, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        this.responseCode = responseCode;
        this.channel = channel;
        this.envelope = envelope;
        this.properties = properties;
        this.body = body;
    }

    public static HandlerResponse ok(Channel channel, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        return new HandlerResponse(ResponseCode.OK, channel, envelope, properties, body);
    }

    public Channel getChannel() {
        return channel;
    }

    public static enum ResponseCode {
        OK,
        INTERRUPTED,
        EXCEPTION, ANCIENT
    }

    private ResponseCode responseCode;
    private final Channel channel;
    private final Envelope envelope;
    private final AMQP.BasicProperties properties;
    private final byte[] body;

    public ResponseCode getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(ResponseCode responseCode) {
        this.responseCode = responseCode;
    }

    public boolean isSuccess() {
        return responseCode == ResponseCode.OK;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public AMQP.BasicProperties getProperties() {
        return properties;
    }
}
