package com.xing.beetle.amqp;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import com.xing.beetle.util.ExceptionSupport;

import static java.util.Objects.requireNonNull;

public class MsgDeliveryTagMapping {

    private class MappingConsumer implements Consumer {

        private Consumer delegate;
        private Channel channel;

        MappingConsumer(Consumer delegate, Channel channel) {
            this.delegate = requireNonNull(delegate);
            this.channel = requireNonNull(channel);
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
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            envelope = mapEnvelope(channel, envelope);
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

    private interface Acknowledgement {

        Acknowledgement NOOP = (mode, multiple, requeue, when) -> null;

        Void perform(Mode mode, boolean multiple, boolean requeue, Predicate<Channel> when) throws IOException;
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

    public MsgDeliveryTagMapping() {
        this.deliveryTagGenerator = new AtomicLong();
        this.deliveryTags = new ConcurrentSkipListMap<>();
    }

    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        Map<Long, Acknowledgement> acks = multiple ? deliveryTags.headMap(deliveryTag, true).descendingMap() : deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        doWithEachChannelOnlyOnce(acks, Mode.ACK, multiple, false);
        acks.clear();
    }

    private void doWithEachChannelOnlyOnce(Map<Long, Acknowledgement> acks, Mode mode, boolean multiple, boolean requeue) throws IOException {
        if (acks.isEmpty()) {
            throw new IOException("Unknown delivery tag");
        }
        Set<Channel> alreadyUsed = new HashSet<>();
        acks.forEach((ExceptionSupport.BiConsumer<Long, Acknowledgement>) (tag, ack) -> {
            ack.perform(Mode.ACK, multiple, requeue, alreadyUsed::add);
        });
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        Map<Long, Acknowledgement> acks = multiple ? deliveryTags.headMap(deliveryTag, true).descendingMap() : deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        doWithEachChannelOnlyOnce(acks, Mode.NACK, multiple, requeue);
        acks.clear();
    }

    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
        Map<Long, Acknowledgement> acks = deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        doWithEachChannelOnlyOnce(acks, Mode.REJECT, false, requeue);
        acks.clear();
    }

    public long mapDelivery(Channel channel, long deliveryTag) {
        long tag = deliveryTagGenerator.incrementAndGet();
        deliveryTags.put(tag,
                (mode, multiple, requeue, when) -> when.test(channel) ? mode.invoke(channel, deliveryTag, multiple, requeue) : null);
        return tag;
    }

    public Envelope mapEnvelope(Channel channel, Envelope envelope) {
        long tag = mapDelivery(channel, envelope.getDeliveryTag());
        return new Envelope(tag, envelope.isRedeliver(), envelope.getExchange(), envelope.getRoutingKey());
    }

    public GetResponse mapResponse(Channel channel, GetResponse response) {
        Envelope envelope = mapEnvelope(channel, response.getEnvelope());
        return new GetResponse(envelope, response.getProps(), response.getBody(), response.getMessageCount());
    }

    public Consumer createConsumerDecorator(Consumer delegate, Channel channel) {
        return new MappingConsumer(delegate, channel);
    }
}
