package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import com.xing.beetle.util.ExceptionSupport;
import com.xing.beetle.util.ExceptionSupport.Function;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class MultiPlexingConnection implements DefaultConnection.Decorator {

  public static class MultiPlexingChannel implements DefaultChannel.Decorator {

    private final Connection connection;
    private final Map<String, Channel> consumerTags;
    private final MsgDeliveryTagMapping tagMapping;
    private final Set<ConfirmListener> confirmListeners;
    private final Channel publisher;

    private volatile com.rabbitmq.client.Consumer defaultConsumer;

    public MultiPlexingChannel(Connection connection) throws IOException {
      this(connection, -1);
    }

    MultiPlexingChannel(Connection connection, int channelNumber) throws IOException {
      this.connection = requireNonNull(connection);
      this.consumerTags = new ConcurrentHashMap<>();
      this.tagMapping = new MsgDeliveryTagMapping();
      this.confirmListeners = Collections.synchronizedSet(new HashSet<>());
      this.publisher =
          channelNumber >= 0 ? connection.createChannel(channelNumber) : connection.createChannel();
    }

    @Override
    public void abort(int closeCode, String closeMessage) throws IOException {
      List<Throwable> exceptions = new ArrayList<>();
      ExceptionSupport.Consumer<Channel> aborting = c -> c.abort(closeCode, closeMessage);
      aborting.executeAndCatch(publisher).ifPresent(exceptions::add);
      aborting.mapAndCatch(consumerTags.values().stream()).forEach(exceptions::add);
      if (!exceptions.isEmpty()) {
        ExceptionSupport.sneakyThrow(exceptions.get(0));
      }
    }

    @Override
    public void addConfirmListener(ConfirmListener listener) {
      if (confirmListeners.add(requireNonNull(listener))) {
        consumerTags.values().forEach(c -> c.addConfirmListener(listener));
      }
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
      tagMapping.basicAck(deliveryTag, multiple);
    }

    @Override
    public void basicCancel(String consumerTag) throws IOException {
      Channel consumer = consumerTags.remove(consumerTag);
      if (consumer != null) {
        consumer.basicCancel(consumerTag);
      }
    }

    @Override
    public String basicConsume(
        String queue,
        boolean autoAck,
        String consumerTag,
        boolean noLocal,
        boolean exclusive,
        Map<String, Object> arguments,
        com.rabbitmq.client.Consumer callback)
        throws IOException {
      consumerTag =
          consumerTag == null || consumerTag.isEmpty() ? UUID.randomUUID().toString() : consumerTag;
      Channel channel = consumerTags.computeIfAbsent(consumerTag, this::newConsumer);
      if (!autoAck) {
        callback = tagMapping.createConsumerDecorator(callback, channel);
      }
      return channel.basicConsume(
          queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
      if (autoAck) {
        return publisher.basicGet(queue, true);
      } else {
        return tagMapping.mapResponse(publisher, publisher.basicGet(queue, false));
      }
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
      tagMapping.basicNack(deliveryTag, multiple, requeue);
    }

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
      consumerTags
          .values()
          .forEach(
              (ExceptionSupport.Consumer<Channel>)
                  ch -> ch.basicQos(prefetchSize, prefetchCount, global));
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
      tagMapping.basicReject(deliveryTag, requeue);
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
      List<Throwable> exceptions = new ArrayList<>();
      ExceptionSupport.Consumer<Channel> closing = c -> c.close(closeCode, closeMessage);
      closing.executeAndCatch(publisher).ifPresent(exceptions::add);
      closing.mapAndCatch(consumerTags.values().stream()).forEach(exceptions::add);
      if (!exceptions.isEmpty()) {
        ExceptionSupport.sneakyThrow(exceptions.get(0));
      }
    }

    @Override
    public <R> R delegateMap(Type type, Function<Channel, ? extends R> fn) {
      switch (type) {
        case CONSUME:
          return consumerTags.values().stream()
              .map(fn)
              .reduce(null, (r1, r2) -> r1 != null ? r1 : r2);
        default:
          return fn.apply(publisher);
      }
    }

    private void ensureOpen() {
      if (!publisher.isOpen()) {
        throw new AlreadyClosedException(publisher.getCloseReason());
      }
    }

    @Override
    public com.rabbitmq.client.Consumer getDefaultConsumer() {
      return defaultConsumer;
    }

    private Channel newConsumer(String tag) {
      ensureOpen();
      try {
        Channel channel = connection.createChannel();
        channel.setDefaultConsumer(defaultConsumer);
        confirmListeners.forEach(channel::addConfirmListener);
        return channel;
      } catch (IOException e) {
        return ExceptionSupport.sneakyThrow(e);
      }
    }

    @Override
    public void notifyListeners() {
      consumerTags.values().forEach(Channel::notifyListeners);
    }

    @Override
    public boolean removeConfirmListener(ConfirmListener listener) {
      if (confirmListeners.remove(listener)) {
        return consumerTags.values().stream().allMatch(c -> c.removeConfirmListener(listener));
      } else {
        return false;
      }
    }

    @Override
    public void setDefaultConsumer(com.rabbitmq.client.Consumer consumer) {
      this.defaultConsumer = consumer;
      consumerTags.values().forEach(c -> c.setDefaultConsumer(consumer));
    }
  }

  private final Connection delegate;

  public MultiPlexingConnection(Connection delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  public Channel createChannel(int channelNumber) throws IOException {
    return new MultiPlexingChannel(delegate, channelNumber);
  }

  @Override
  public <R> R delegateMap(Function<Connection, ? extends R> con) {
    return con.apply(delegate);
  }
}
