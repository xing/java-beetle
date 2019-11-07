package com.xing.beetle.amqp;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.xing.beetle.BeetleHeader;
import com.xing.beetle.util.RingStream;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class BeetleChannel implements ChannelDecorator.Multiple {

    private static final Logger LOGGER = System.getLogger(BeetleChannel.class.getName());

    private final RingStream<Channel> delegates;
    private final MsgDeliveryTagMapping tagMapping;

    public BeetleChannel(List<Channel> channels) {
        this.delegates = new RingStream<>(channels.toArray(new Channel[channels.size()]));
        this.tagMapping = new MsgDeliveryTagMapping();
    }

    @Override
    public MsgDeliveryTagMapping deliveryTagMapping() {
        return tagMapping;
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        tagMapping.basicAck(deliveryTag, multiple);
    }

    @Override
    public void basicCancel(String consumerTag) throws IOException {
        delegateForEach(c -> c.basicCancel(consumerTag));
    }

    @Override
    public long messageCount(String queue) throws IOException {
        return delegateMap(c->c.messageCount(queue)).mapToLong(Long::longValue).sum();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
                               boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
        String tag =
                consumerTag == null || consumerTag.isEmpty() ? UUID.randomUUID().toString() : consumerTag;
        boolean all = delegateMap(
                c -> c.basicConsume(queue, autoAck, tag, noLocal, exclusive, arguments,tagMapping.createConsumerDecorator(callback, c)))
                .allMatch(tag::equals);
        if (!all) {
            throw new AssertionError("Returned consumer tags dont match");
        }
        return tag;
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        tagMapping.basicNack(deliveryTag, multiple, requeue);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate,
                             BasicProperties props, byte[] body) throws IOException {
        int redundancy = 1;
        if (props != null && props.getHeaders() != null) {
            redundancy = (int) props.getHeaders().getOrDefault(BeetleHeader.PUBLISH_REDUNDANCY, redundancy);
        }
        long sent = delegates.streamAll()
                .filter(c -> send(c, exchange, routingKey, mandatory, immediate, props, body))
                .limit(redundancy).count();
        if (sent == 0) {
            throw new IOException("Unable to sent the message to any broker. Message Header: " + props);
        }
        if (sent != redundancy) {
            LOGGER.log(Level.WARNING, "Message was sent " + sent + " times. Expected was a redundancy of "
                    + redundancy + ". Message Header:" + props);
        }
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
        tagMapping.basicReject(deliveryTag, requeue);
    }

    @Override
    public Stream<? extends Channel> delegates() {
        return delegates.streamAll();
    }

    @Override
    public long getNextPublishSeqNo() {
        throw new UnsupportedOperationException();
    }

    private Stream<? extends Channel> nextOpenChannels(int count) {
        return delegates().filter(Channel::isOpen).limit(count);
    }

    private boolean send(Channel channel, String exchange, String routingKey, boolean mandatory,
                         boolean immediate, BasicProperties props, byte[] body) {
        try {
            channel.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, String.format("Failed to send message with headers %s to %s", props,
                    channel.getConnection().getAddress()), e);
            return false;
        }
    }
}
