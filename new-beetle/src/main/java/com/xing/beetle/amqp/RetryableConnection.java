package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.RecoveryListener;
import com.xing.beetle.util.OrderedPromise;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

public class RetryableConnection implements ConnectionDecorator.Async, RecoveryListener {

  private class RetryableChannel implements ChannelDecorator.Async {

    private final OrderedPromise<Channel> channel;

    RetryableChannel(OrderedPromise<Channel> channel) {
      this.channel = requireNonNull(channel);
    }

    @Override
    public void basicPublish(
        String exchange,
        String routingKey,
        boolean mandatory,
        boolean immediate,
        BasicProperties props,
        byte[] body)
        throws IOException {
      Channel c =
          channel.get().orElseThrow(() -> new IOException("Connection not yet established"));
      c.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
    }

    @Override
    public <R> CompletionStage<R> execute(Action<? super Channel, ? extends R> action) {
      return channel.<R>thenApply(action::doExecute).toStage();
    }

    @Override
    public boolean isOpen() {
      return RetryableConnection.this.isOpen();
    }
  }

  private final OrderedPromise<RecoverableConnection> connection;

  private volatile boolean active;

  public RetryableConnection(CompletionStage<RecoverableConnection> connection) {
    this.connection = new OrderedPromise<>(connection);
    connection.thenAccept(c -> c.addRecoveryListener(this));
    connection.thenAccept(c -> active = c.isOpen());
  }

  @Override
  public Channel createChannel(int channelNumber) {
    return new RetryableChannel(
        connection.thenApply(
            c -> channelNumber >= 0 ? c.createChannel(channelNumber) : c.createChannel()));
  }

  @Override
  public <R> CompletionStage<R> execute(Action<? super Connection, ? extends R> action) {
    return connection.<R>thenApply(action::doExecute).toStage();
  }

  @Override
  public void handleRecovery(Recoverable recoverable) {
    active = true;
  }

  @Override
  public void handleRecoveryStarted(Recoverable recoverable) {
    active = false;
  }

  @Override
  public boolean isOpen() {
    return active;
  }
}
