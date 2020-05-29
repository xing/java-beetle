package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.xing.beetle.util.ExceptionSupport.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** BeetleConnection aggregates connections to one or more brokers to one virtual connection. */
public class BeetleConnection implements DefaultConnection.Decorator, ShutdownListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeetleConnection.class);

  private final List<? extends Connection> delegates;
  private final Set<ShutdownListener> shutdownListeners;

  /**
   * Build a new BeetleConnection that aggregfates the given connections.
   *
   * @param connections Wrapped AMQP connections.
   */
  public BeetleConnection(List<Connection> connections) {
    this.delegates = new ArrayList<>(connections);
    this.shutdownListeners = new HashSet<>();
    connections.forEach(c -> c.addShutdownListener(this));
  }

  @Override
  public void addShutdownListener(ShutdownListener listener) {
    shutdownListeners.add(requireNonNull(listener));
  }

  @Override
  public Channel createChannel(int channelNumber) throws IOException {
    List<Channel> channels = new ArrayList<>();
    for (Connection connection : delegates) {
      channels.add(
          channelNumber >= 0
              ? connection.createChannel(channelNumber)
              : connection.createChannel());
    }
    return new BeetleChannel(channels);
  }

  @Override
  public <R> R delegateMap(Function<Connection, ? extends R> fn) {
    return delegates.stream().map(fn).reduce(null, (r1, r2) -> r1 != null ? r1 : r2);
  }

  @Override
  public void removeShutdownListener(ShutdownListener listener) {
    shutdownListeners.remove(listener);
  }

  @Override
  public void shutdownCompleted(ShutdownSignalException cause) {
    shutdownListeners.forEach(l -> l.shutdownCompleted(cause));
    if (cause.isHardError()) {
      Connection connection = (Connection) cause.getReference();
      if (cause.isInitiatedByApplication()) {
        LOGGER.debug("Connection {} closed because of {}", connection, cause.getReason());
      } else {
        LOGGER.warn(
            "Connection to the broker at {}:{} lost, reconnecting in 10 seconds. Reason: {}",
            connection.getAddress(),
            connection.getPort(),
            cause.getReason());
      }
    } else {
      Channel channel = (Channel) cause.getReference();
      LOGGER.info(
          "AQMP channel shutdown {} because of {}. Doing nothing about it.",
          channel,
          cause.getReason());
    }
  }
}
