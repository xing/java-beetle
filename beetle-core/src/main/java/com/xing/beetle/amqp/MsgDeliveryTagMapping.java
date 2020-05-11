package com.xing.beetle.amqp;

import com.rabbitmq.client.*;
import com.xing.beetle.util.ExceptionSupport;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class MsgDeliveryTagMapping {

  private interface Acknowledgement {

    Void perform(Mode mode, boolean multiple, boolean requeue, Predicate<Channel> when)
        throws IOException;
  }

  private class MappingConsumer implements Consumer {

    private final Consumer delegate;
    private final Channel channel;

    MappingConsumer(Consumer delegate, Channel channel) {
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
      envelope = mapEnvelope(channel, envelope);
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
  }

  enum Mode {
    ACK {
      @Override
      Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
          throws IOException {
        channel.basicAck(deliveryTag, multiple);
        return null;
      }
    },
    NACK {
      @Override
      Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
          throws IOException {
        channel.basicNack(deliveryTag, multiple, requeue);
        return null;
      }
    },
    REJECT {
      @Override
      Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
          throws IOException {
        channel.basicReject(deliveryTag, requeue);
        return null;
      }
    };

    abstract Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
        throws IOException;
  }

  private final AtomicLong deliveryTagGenerator;
  private final ConcurrentNavigableMap<Long, Acknowledgement> deliveryTags;

  MsgDeliveryTagMapping() {
    this.deliveryTagGenerator = new AtomicLong();
    this.deliveryTags = new ConcurrentSkipListMap<>();
  }

  public void basicAck(long deliveryTag, boolean multiple) throws IOException {
    Map<Long, Acknowledgement> acks =
        multiple
            ? deliveryTags.headMap(deliveryTag, true).descendingMap()
            : deliveryTags.subMap(deliveryTag, deliveryTag + 1);
    doWithEachChannelOnlyOnce(acks, Mode.ACK, multiple, false);
    acks.clear();
  }

  void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
    Map<Long, Acknowledgement> acks =
        multiple
            ? deliveryTags.headMap(deliveryTag, true).descendingMap()
            : deliveryTags.subMap(deliveryTag, deliveryTag + 1);
    doWithEachChannelOnlyOnce(acks, Mode.NACK, multiple, requeue);
    acks.clear();
  }

  void basicReject(long deliveryTag, boolean requeue) throws IOException {
    Map<Long, Acknowledgement> acks = deliveryTags.subMap(deliveryTag, deliveryTag + 1);
    doWithEachChannelOnlyOnce(acks, Mode.REJECT, false, requeue);
    acks.clear();
  }

  Consumer createConsumerDecorator(Consumer delegate, Channel channel) {
    return new MappingConsumer(delegate, channel);
  }

  private void doWithEachChannelOnlyOnce(
      Map<Long, Acknowledgement> acks, Mode mode, boolean multiple, boolean requeue)
      throws IOException {
    if (acks.isEmpty()) {
      throw new IOException("Unknown delivery tag");
    }
    Set<Channel> alreadyUsed = new HashSet<>();
    acks.forEach(
        (ExceptionSupport.BiConsumer<Long, Acknowledgement>)
            (tag, ack) -> {
              ack.perform(mode, multiple, requeue, alreadyUsed::add);
            });
  }

  private long mapDelivery(Channel channel, long deliveryTag) {
    long tag = deliveryTagGenerator.incrementAndGet();
    deliveryTags.put(
        tag,
        (mode, multiple, requeue, when) ->
            when.test(channel) ? mode.invoke(channel, deliveryTag, multiple, requeue) : null);
    return tag;
  }

  Envelope mapEnvelope(Channel channel, Envelope envelope) {
    long tag = mapDelivery(channel, envelope.getDeliveryTag());
    return new Envelope(
        tag, envelope.isRedeliver(), envelope.getExchange(), envelope.getRoutingKey());
  }

  GetResponse mapResponse(Channel channel, GetResponse response) {
    if (response != null) {
      Envelope envelope = mapEnvelope(channel, response.getEnvelope());
      return new GetResponse(
          envelope, response.getProps(), response.getBody(), response.getMessageCount());
    } else {
      return null;
    }
  }
}
