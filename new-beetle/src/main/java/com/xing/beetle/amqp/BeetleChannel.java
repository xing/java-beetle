package com.xing.beetle.amqp;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.xing.beetle.util.RingStream;

public class BeetleChannel implements ChannelDecorator.Multiple {

  private static class Metadata {

    private final Map<String, String> consumerTags;

    Metadata() {
      this.consumerTags = new ConcurrentHashMap<>();
    }

    void subscribe(String virtualConsumerTag, String actualConsumerTag) {
      consumerTags.putIfAbsent(virtualConsumerTag, actualConsumerTag);
    }

    Optional<String> unsubscribe(String consumerTag) {
      return Optional.ofNullable(consumerTags.remove(consumerTag));
    }
  }

  private static final Logger LOGGER = System.getLogger(BeetleChannel.class.getName());

  public static final String REDUNDANT_HEADER_KEY = "x-redundancy";

  private final RingStream<Channel> delegates;
  private ConcurrentMap<Channel, Metadata> metadata;

  public BeetleChannel(List<Channel> channels) {
    this.delegates = new RingStream<>(channels.toArray(new Channel[channels.size()]));
  }

  @Override
  public void basicAck(long deliveryTag, boolean multiple) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void basicCancel(String consumerTag) throws IOException {
    // TODO Auto-generated method stub
    delegateForEach(c -> c.basicCancel(consumerTag));
  }

  @Override
  public String basicConsume(
      String queue,
      boolean autoAck,
      String consumerTag,
      boolean noLocal,
      boolean exclusive,
      Map<String, Object> arguments,
      Consumer callback)
      throws IOException {
    String tag =
        consumerTag == null || consumerTag.isEmpty() ? UUID.randomUUID().toString() : consumerTag;
    boolean all =
        delegateMap(
                c -> c.basicConsume(queue, autoAck, tag, noLocal, exclusive, arguments, callback))
            .allMatch(tag::equals);
    if (!all) {
      LOGGER.log(Level.WARNING, "Failed to register consumer for all brokers");
    }
    return tag;
  }

  @Override
  public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
    // TODO Auto-generated method stub
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
    int redundancy = 1;
    if (props != null && props.getHeaders() != null) {
      redundancy = (int) props.getHeaders().getOrDefault(REDUNDANT_HEADER_KEY, redundancy);
    }
    long sent =
        delegates
            .streamAll()
            .filter(c -> send(c, exchange, routingKey, mandatory, immediate, props, body))
            .limit(redundancy)
            .count();
    if (sent == 0) {
      throw new IOException("Unable to sent the message to any broker. Message Header: " + props);
    }
    if (sent != redundancy) {
      LOGGER.log(
          Level.WARNING,
          "Message was sent "
              + sent
              + " times. Expected was a redundancy of "
              + redundancy
              + ". Message Header:"
              + props);
    }
  }

  @Override
  public void basicReject(long deliveryTag, boolean requeue) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public Stream<? extends Channel> delegates() {
    return delegates.streamAll();
  }

  @Override
  public long getNextPublishSeqNo() {
    // TODO Auto-generated method stub
    return 0;
  }

  private Stream<? extends Channel> nextOpenChannels(int count) {
    return delegates().filter(Channel::isOpen).limit(count);
  }

  private boolean send(
      Channel channel,
      String exchange,
      String routingKey,
      boolean mandatory,
      boolean immediate,
      BasicProperties props,
      byte[] body) {
    try {
      channel.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
      return true;
    } catch (Exception e) {
      LOGGER.log(
          Level.WARNING,
          String.format(
              "Failed to send message with headers %s to %s",
              props, channel.getConnection().getAddress()),
          e);
      return false;
    }
  }
}
