package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.RecoveryListener;
import com.xing.beetle.util.ExceptionSupport.Function;
import java.io.IOException;

public class RetryableConnection implements DefaultConnection.Decorator, RecoveryListener {

  private class RetryableChannel implements DefaultChannel.Decorator {

    private final Channel channel;

    RetryableChannel(Channel channel) {
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
      channel.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
    }

    @Override
    public <R> R delegateMap(Type type, Function<Channel, ? extends R> fn) {
      return fn.apply(channel);
    }

    @Override
    public boolean isOpen() {
      return RetryableConnection.this.isOpen();
    }
  }

  private final RecoverableConnection connection;

  private volatile boolean active;

  public RetryableConnection(RecoverableConnection connection) {
    this.connection = connection;
    connection.addRecoveryListener(this);
    this.active = connection.isOpen();
  }

  @Override
  public Channel createChannel(int channelNumber) throws IOException {
    return new RetryableChannel(
        channelNumber >= 0 ? connection.createChannel(channelNumber) : connection.createChannel());
  }

  @Override
  public <R> R delegateMap(Function<Connection, ? extends R> con) {
    return con.apply(connection);
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
