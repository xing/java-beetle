package com.xing.beetle.amqp;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.GetResponse;
import com.xing.beetle.BeetleHeader;
import com.xing.beetle.util.ExceptionSupport;
import com.xing.beetle.util.ExceptionSupport.Function;
import com.xing.beetle.util.RingStream;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.*;

/** BeetleChannel wraps one or more actual AMQP channels for consumption by a message processor. */
public class BeetleChannel implements DefaultChannel.Decorator {

  private static final Logger LOGGER = System.getLogger(BeetleChannel.class.getName());
  private static final int FLAG_REDUNDANT = 1;

  private final RingStream<Channel> delegates;
  private final BeetleAmqpConfiguration configuration;
  private final MsgDeliveryTagMapping tagMapping;

  BeetleChannel(List<Channel> channels, BeetleAmqpConfiguration configuration) {
    this.delegates = new RingStream<>(channels.toArray(new Channel[channels.size()]));
    this.configuration = configuration;
    this.tagMapping = new MsgDeliveryTagMapping();
  }

  @Override
  public void basicAck(long deliveryTag, boolean multiple) throws IOException {
    tagMapping.basicAck(deliveryTag, multiple);
  }

  @Override
  public void basicQos(int prefetchCount) throws IOException {
    delegates
        .streamAll()
        .forEach((ExceptionSupport.Consumer<Channel>) ch -> ch.basicQos(prefetchCount));
  }

  @Override
  public void basicQos(int prefetchCount, boolean global) throws IOException {
    delegates
        .streamAll()
        .forEach((ExceptionSupport.Consumer<Channel>) ch -> ch.basicQos(prefetchCount, global));
  }

  @Override
  public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
    delegates
        .streamAll()
        .forEach(
            (ExceptionSupport.Consumer<Channel>)
                ch -> ch.basicQos(prefetchSize, prefetchCount, global));
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
        delegates
            .streamAll()
            .map(
                (ExceptionSupport.Function<Channel, String>)
                    ch ->
                        ch.basicConsume(
                            queue,
                            autoAck,
                            tag,
                            noLocal,
                            exclusive,
                            arguments,
                            tagMapping.createConsumerDecorator(callback, ch)))
            .allMatch(tag::equals);
    if (!all) {
      throw new AssertionError("Returned consumer tags dont match");
    }
    return tag;
  }

  @Override
  public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
    return delegates
        .streamAll()
        .map(
            (ExceptionSupport.Function<Channel, GetResponse>)
                ch -> tagMapping.mapResponse(ch, ch.basicGet(queue, autoAck)))
        .filter(Objects::nonNull)
        .findAny()
        .orElse(null);
  }

  @Override
  public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
    tagMapping.basicNack(deliveryTag, multiple, requeue);
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

    if (props == null) {
      props = new BasicProperties();
      Map<String, Object> headers = new HashMap<>();
      props = props.builder().headers(headers).build();
    }

    int redundancy = (int) props.getHeaders().getOrDefault(BeetleHeader.PUBLISH_REDUNDANCY, 1);
    if (redundancy > 1 && props.getMessageId() == null) {
      props = props.builder().messageId(UUID.randomUUID().toString()).build();
    }

    BasicProperties properties;
    Map<String, Object> headers = new HashMap<>(props.getHeaders());
    headers.put("flags", redundancy > 1 ? FLAG_REDUNDANT : 0);
    if (!headers.containsKey(BeetleHeader.EXPIRES_AT)) {
      headers.put(
          BeetleHeader.EXPIRES_AT, System.currentTimeMillis() + configuration.getMessageLifetime());
    }
    properties = props.builder().headers(headers).build();

    long sent =
        delegates
            .streamAll()
            .filter(c -> send(c, exchange, routingKey, mandatory, immediate, properties, body))
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
              + properties);
    }
  }

  @Override
  public void basicReject(long deliveryTag, boolean requeue) throws IOException {
    tagMapping.basicReject(deliveryTag, requeue);
  }

  @Override
  public <R> R delegateMap(Type type, Function<Channel, ? extends R> ch) {
    return delegates.streamAll().map(ch).reduce(null, (r1, r2) -> r1 != null ? r1 : r2);
  }

  @Override
  public long getNextPublishSeqNo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long messageCount(String queue) throws IOException {
    return delegates
        .streamAll()
        .map((ExceptionSupport.Function<Channel, Long>) ch -> ch.messageCount(queue))
        .mapToLong(Long::longValue)
        .sum();
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
      System.out.println("sent " + channel.hashCode() + " " + props.getMessageId());
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
