package com.xing.beetle.amqp;

import com.rabbitmq.client.*;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * MappingConsumer wraps a plain Consumer to support consuming messages from multiple channels
 * without conflicting deliveryTags.
 */
class MappingConsumer implements Consumer {

  private final MsgDeliveryTagMapping msgDeliveryTagMapping;
  private final Consumer delegate;
  private final Channel channel;

  MappingConsumer(MsgDeliveryTagMapping msgDeliveryTagMapping, Consumer delegate, Channel channel) {
    this.msgDeliveryTagMapping = msgDeliveryTagMapping;
    this.delegate = requireNonNull(delegate);
    this.channel = requireNonNull(channel);
  }

  @Override
  public void handleCancel(String consumerTag) throws IOException {
    delegate.handleCancel(consumerTag);
  }

  @Override
  public void handleCancelOk(String consumerTag) {
    delegate.handleCancelOk(consumerTag);
  }

  @Override
  public void handleConsumeOk(String consumerTag) {
    delegate.handleConsumeOk(consumerTag);
  }

  @Override
  public void handleDelivery(
      String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
      throws IOException {
    envelope = msgDeliveryTagMapping.envelopeWithPseudoDeliveryTag(channel, envelope);
    delegate.handleDelivery(consumerTag, envelope, properties, body);
  }

  @Override
  public void handleRecoverOk(String consumerTag) {
    delegate.handleRecoverOk(consumerTag);
  }

  @Override
  public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
    delegate.handleShutdownSignal(consumerTag, sig);
  }

  public Consumer getDelegate() {
    return delegate;
  }

  public Channel getChannel() {
    return channel;
  }
}
