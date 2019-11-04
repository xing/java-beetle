package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Confirm.SelectOk;
import com.xing.beetle.util.ExceptionSupport;
import com.xing.beetle.util.ExceptionSupport.Consumer;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class MultiPlexingChannel implements ChannelDecorator {

  private final Connection connection;
  private final Map<String, Channel> consumerTags;
  private final Set<ConfirmListener> confirmListeners;
  private final Channel publisher;

  private volatile com.rabbitmq.client.Consumer defaultConsumer;

  public MultiPlexingChannel(Connection connection) throws IOException {
    this(connection, -1);
  }

  public MultiPlexingChannel(Connection connection, int channelNumber) throws IOException {
    this.connection = requireNonNull(connection);
    this.consumerTags = new ConcurrentHashMap<>();
    this.confirmListeners = Collections.synchronizedSet(new HashSet<>());
    this.publisher =
        channelNumber >= 0 ? connection.createChannel(channelNumber) : connection.createChannel();
  }

  @Override
  public void abort(int closeCode, String closeMessage) throws IOException {
    List<Exception> exceptions = new ArrayList<>();
    Consumer<Channel> aborting = c -> c.abort(closeCode, closeMessage);
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
  public void addReturnListener(ReturnListener listener) {
    publisher.addReturnListener(requireNonNull(listener));
  }

  @Override
  public void addShutdownListener(ShutdownListener listener) {
    publisher.addShutdownListener(requireNonNull(listener));
  }

  @Override
  public CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
    return publisher.asyncCompletableRpc(method);
  }

  @Override
  public void asyncRpc(Method method) throws IOException {
    publisher.asyncRpc(method);
  }

  @Override
  public void basicCancel(String consumerTag) throws IOException {
    Channel consumer = consumerTags.remove(consumerTag);
    if (consumer != null) {
      consumer.basicCancel(consumerTag);
      try {
        consumer.close();
      } catch (TimeoutException e) {
        ExceptionSupport.sneakyThrow(e);
      }
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
    Channel channel = consumerTags.computeIfAbsent(consumerTag, this::newConsumer);
    return channel.basicConsume(
        queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
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
    publisher.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
  }

  @Override
  public void clearConfirmListeners() {
    consumerTags.values().forEach(Channel::clearConfirmListeners);
  }

  @Override
  public void clearReturnListeners() {
    publisher.clearReturnListeners();
  }

  @Override
  public void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
    List<Exception> exceptions = new ArrayList<>();
    Consumer<Channel> closing = c -> c.close(closeCode, closeMessage);
    closing.executeAndCatch(publisher).ifPresent(exceptions::add);
    closing.mapAndCatch(consumerTags.values().stream()).forEach(exceptions::add);
    if (!exceptions.isEmpty()) {
      ExceptionSupport.sneakyThrow(exceptions.get(0));
    }
  }

  @Override
  public SelectOk confirmSelect() throws IOException {
    return publisher.confirmSelect();
  }

  @Override
  public long consumerCount(String queue) throws IOException {
    return publisher.consumerCount(queue);
  }

  private void ensureOpen() {
    if (!publisher.isOpen()) {
      throw new AlreadyClosedException(publisher.getCloseReason());
    }
  }

  @Override
  public int getChannelNumber() {
    return publisher.getChannelNumber();
  }

  @Override
  public ShutdownSignalException getCloseReason() {
    return publisher.getCloseReason();
  }

  @Override
  public Connection getConnection() {
    return publisher.getConnection();
  }

  @Override
  public com.rabbitmq.client.Consumer getDefaultConsumer() {
    return defaultConsumer;
  }

  @Override
  public long getNextPublishSeqNo() {
    return publisher.getNextPublishSeqNo();
  }

  @Override
  public boolean isOpen() {
    return publisher.isOpen();
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
    publisher.notifyListeners();
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
  public boolean removeReturnListener(ReturnListener listener) {
    return publisher.removeReturnListener(listener);
  }

  @Override
  public void removeShutdownListener(ShutdownListener listener) {
    publisher.removeShutdownListener(listener);
  }

  @Override
  public Command rpc(Method method) throws IOException {
    return publisher.rpc(method);
  }

  @Override
  public void setDefaultConsumer(com.rabbitmq.client.Consumer consumer) {
    this.defaultConsumer = consumer;
    consumerTags.values().forEach(c -> c.setDefaultConsumer(consumer));
  }

  @Override
  public boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
    return publisher.waitForConfirms(timeout);
  }
}
