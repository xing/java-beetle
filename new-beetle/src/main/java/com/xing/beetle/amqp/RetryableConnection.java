package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.RecoveryListener;
import com.xing.beetle.util.ExceptionSupport.Function;
import com.xing.beetle.util.OrderedPromise;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

public class RetryableConnection implements DefaultConnection.Decorator, RecoveryListener {

  private class RetryableChannel implements DefaultChannel.Decorator {

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
          channel.getNow().orElseThrow(() -> new IOException("Connection not yet established"));
      c.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
    }

    @Override
    public <R> R delegateMap(Type type, Function<Channel, ? extends R> fn) {
      return channel.thenApply(fn).join();
    }

    @Override
    public boolean isOpen() {
      return RetryableConnection.this.isOpen();
    }
  }

  private final OrderedPromise<RecoverableConnection> connection;

  private volatile boolean active;

  public RetryableConnection(CompletionStage<RecoverableConnection> connection) {
    this.connection = OrderedPromise.of(connection);
    connection.thenAccept(c -> c.addRecoveryListener(this));
    connection.thenAccept(c -> active = c.isOpen());
  }

  @Override
  public Channel createChannel(int channelNumber) {
    return new RetryableChannel(
        connection.thenApply(
            con -> channelNumber >= 0 ? con.createChannel(channelNumber) : con.createChannel()));
  }

  @Override
  public <R> R delegateMap(Function<Connection, ? extends R> con) {
    return connection.thenApply(con).join();
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
