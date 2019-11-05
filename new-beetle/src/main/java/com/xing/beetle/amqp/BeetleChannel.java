package com.xing.beetle.amqp;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.xing.beetle.BeetleHeader;
import com.xing.beetle.util.RingStream;

import static java.util.Objects.requireNonNull;

public class BeetleChannel implements ChannelDecorator.Multiple {

    //TODO serialize calls from multiple brokers
    private static class MappingConsumer implements Consumer {

        private Consumer delegate;
        private Channel channel;
        private MsgDeliveryTagMapping tagMapping;

        MappingConsumer(Consumer delegate, Channel channel, MsgDeliveryTagMapping tagMapping) {
            this.delegate = requireNonNull(delegate);
            this.channel = requireNonNull(channel);
            this.tagMapping = requireNonNull(tagMapping);
        }

        @Override
        public void handleConsumeOk(String consumerTag) {
            delegate.handleConsumeOk(consumerTag);
        }

        @Override
        public void handleCancelOk(String consumerTag) {
            delegate.handleCancelOk(consumerTag);
        }

        @Override
        public void handleCancel(String consumerTag) throws IOException {
            delegate.handleCancel(consumerTag);
        }

        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            delegate.handleShutdownSignal(consumerTag, sig);
        }

        @Override
        public void handleRecoverOk(String consumerTag) {
            delegate.handleRecoverOk(consumerTag);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
            envelope = tagMapping.mapEnvelope(channel, envelope);
            delegate.handleDelivery(consumerTag, envelope, properties, body);
        }
    }

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

    private final RingStream<Channel> delegates;
    private final ConcurrentMap<Channel, Metadata> metadata;
    private final MsgDeliveryTagMapping tagMapping;

    public BeetleChannel(List<Channel> channels) {
        this.delegates = new RingStream<>(channels.toArray(new Channel[channels.size()]));
        this.metadata = new ConcurrentHashMap<>(channels.size());
        this.tagMapping = new MsgDeliveryTagMapping();
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
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
                               boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
        String tag =
                consumerTag == null || consumerTag.isEmpty() ? UUID.randomUUID().toString() : consumerTag;
        boolean all = delegateMap(
                c -> c.basicConsume(queue, autoAck, tag, noLocal, exclusive, arguments, new MappingConsumer(callback, c, tagMapping)))
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
