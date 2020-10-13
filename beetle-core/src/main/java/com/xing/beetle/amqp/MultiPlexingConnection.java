package com.xing.beetle.amqp;

import com.rabbitmq.client.*;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.Deduplicator;
import com.xing.beetle.util.ExceptionSupport;
import com.xing.beetle.util.ExceptionSupport.Function;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

public class MultiPlexingConnection implements DefaultConnection.Decorator {

  public static class MultiPlexingChannel implements DefaultChannel.Decorator {

    private final Connection connection;
    private final Map<String, Channel> consumerTags;
    private final MsgDeliveryTagMapping tagMapping;
    private final Set<ConfirmListener> confirmListeners;
    private final Channel publisher;
    private final Deduplicator deduplicator;
    private final boolean rejectAndRequeue;

    private volatile com.rabbitmq.client.Consumer defaultConsumer;

    private int qosPrefetchCount = 0;
    private int qosPrefetchSize = 0;
    private boolean qosGlobal = false;

    public MultiPlexingChannel(
        Connection connection, Deduplicator deduplicator, boolean rejectAndRequeue)
        throws IOException {
      this(connection, -1, deduplicator, rejectAndRequeue);
    }

    MultiPlexingChannel(
        Connection connection,
        int channelNumber,
        Deduplicator deduplicator,
        boolean rejectAndRequeue)
        throws IOException {
      this.connection = requireNonNull(connection);
      this.consumerTags = new ConcurrentHashMap<>();
      this.tagMapping = new MsgDeliveryTagMapping();
      this.confirmListeners = Collections.synchronizedSet(new HashSet<>());
      this.publisher =
          channelNumber >= 0 ? connection.createChannel(channelNumber) : connection.createChannel();
      this.deduplicator = deduplicator;
      this.rejectAndRequeue = rejectAndRequeue;
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
    public AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException {
      return publisher.queuePurge(queue);
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
      channel.setDefaultConsumer(callback);
      channel.basicQos(this.qosPrefetchSize, this.qosPrefetchCount, this.qosGlobal);

      Consumer callBackForAll =
          new Consumer() {
            @Override
            public void handleConsumeOk(String consumerTag) {
              callback.handleConsumeOk(consumerTag);
            }

            @Override
            public void handleCancelOk(String consumerTag) {
              callback.handleCancelOk(consumerTag);
            }

            @Override
            public void handleCancel(String consumerTag) throws IOException {
              callback.handleCancel(consumerTag);
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
              callback.handleShutdownSignal(consumerTag, sig);
            }

            @Override
            public void handleRecoverOk(String consumerTag) {
              callback.handleRecoverOk(consumerTag);
            }

            @Override
            public void handleDelivery(
                String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {

              if (callback instanceof MappingConsumer) {
                MappingConsumer mappingConsumer = (MappingConsumer) callback;
                Consumer consumer = mappingConsumer.getDelegate();
                if (consumer.getClass().getName().contains(DefaultChannel.class.getName())) {
                  // beetle client have the full control
                  BeetleMessageAdapter beetleMessageAdapter =
                      new BeetleMessageAdapter(
                          consumerTags.get(consumerTag), autoAck, rejectAndRequeue);

                  Delivery message = new Delivery(envelope, properties, body);
                  if (beetleMessageAdapter.messageId(message) != null) {

                    MessageListener<Delivery> dedup_handle_delivery_called =
                        new MessageListener<>() {
                          @Override
                          public void onMessage(Delivery delivery) throws Throwable {
                            callback.handleDelivery(
                                consumerTag,
                                delivery.getEnvelope(),
                                delivery.getProperties(),
                                delivery.getBody());
                          }
                        };

                    deduplicator.handle(
                        message, queue, beetleMessageAdapter, dedup_handle_delivery_called);
                    return;
                  }
                }
              }
              callback.handleDelivery(consumerTag, envelope, properties, body);
            }
          };

      Consumer consumer = tagMapping.createConsumerDecorator(callBackForAll, channel);
      setDefaultConsumer(consumer);

      return channel.basicConsume(
          queue, false, consumerTag, noLocal, exclusive, arguments, consumer);
    }

    @Override
    public void basicQos(int prefetchCount) throws IOException {
      this.qosPrefetchCount = prefetchCount;
      consumerTags
          .values()
          .forEach((ExceptionSupport.Consumer<Channel>) ch -> ch.basicQos(prefetchCount));
    }

    @Override
    public void basicQos(int prefetchCount, boolean global) throws IOException {
      this.qosPrefetchCount = prefetchCount;
      this.qosGlobal = global;
      consumerTags
          .values()
          .forEach((ExceptionSupport.Consumer<Channel>) ch -> ch.basicQos(prefetchCount, global));
    }

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
      this.qosPrefetchSize = prefetchSize;
      this.qosPrefetchCount = prefetchCount;
      this.qosGlobal = global;
      consumerTags
          .values()
          .forEach(
              (ExceptionSupport.Consumer<Channel>)
                  ch -> ch.basicQos(prefetchSize, prefetchCount, global));
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
      if (autoAck) {
        return publisher.basicGet(queue, true);
      } else {
        return tagMapping.responseWithPseudoDeliveryTag(
            publisher, publisher.basicGet(queue, false));
      }
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
      tagMapping.basicNack(deliveryTag, multiple, requeue);
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
  private final Deduplicator deduplicator;
  private final boolean deadLetteringEnabled;

  public MultiPlexingConnection(
      Connection delegate, Deduplicator deduplicator, boolean deadLetteringEnabled) {
    this.delegate = requireNonNull(delegate);
    this.deduplicator = deduplicator;
    this.deadLetteringEnabled = deadLetteringEnabled;
  }

  @Override
  public Channel createChannel(int channelNumber) throws IOException {
    return new MultiPlexingChannel(delegate, channelNumber, deduplicator, deadLetteringEnabled);
  }

  @Override
  public <R> R delegateMap(Function<Connection, ? extends R> con) {
    return con.apply(delegate);
  }
}
