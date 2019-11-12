package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeetleConnection implements ConnectionDecorator.Multiple, ShutdownListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeetleConnection.class);

  private final List<? extends Connection> delegates;
  private final Set<ShutdownListener> shutdownListeners;

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
  public Stream<? extends Connection> delegates() {
    return delegates.stream();
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
