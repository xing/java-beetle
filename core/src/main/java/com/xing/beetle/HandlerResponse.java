package com.xing.beetle;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

/**
 *
 */
public class HandlerResponse {

    public HandlerResponse(ResponseCode responseCode, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        this.responseCode = responseCode;
        this.envelope = envelope;
        this.properties = properties;
        this.body = body;
    }

    public static HandlerResponse ok(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        return new HandlerResponse(ResponseCode.OK, envelope, properties, body);
    }

    public static enum ResponseCode {
        OK
    }

    private ResponseCode responseCode;
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
