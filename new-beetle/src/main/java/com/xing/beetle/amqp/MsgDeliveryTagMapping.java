package com.xing.beetle.amqp;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.xing.beetle.util.ExceptionSupport;

public class MsgDeliveryTagMapping {

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

    private void doWithEachChannelOnlyOnce(Map<Long, Acknowledgement> acks, Mode mode, boolean multiple, boolean requeue) {
        Set<Channel> alreadyUsed = new HashSet<>();
        acks.forEach((ExceptionSupport.BiConsumer<Long, Acknowledgement>) (tag, ack) -> {
            ack.perform(Mode.ACK, multiple, false, alreadyUsed::add);
        });
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        Map<Long, Acknowledgement> acks = multiple ? deliveryTags.headMap(deliveryTag, true).descendingMap() : deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        doWithEachChannelOnlyOnce(acks, Mode.NACK, multiple, requeue);
        acks.clear();
    }

    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
        Map<Long, Acknowledgement> acks = deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        doWithEachChannelOnlyOnce(acks, Mode.NACK, false, requeue);
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
}
