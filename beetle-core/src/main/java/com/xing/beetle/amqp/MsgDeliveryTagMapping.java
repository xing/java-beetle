package com.xing.beetle.amqp;

import com.rabbitmq.client.*;

import static com.xing.beetle.util.ExceptionSupport.BiConsumer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * MsgDeliveryTagMapping maps external delivery tags from (possibly) multiple AMQP channels to a
 * contiguous list of synthetic delivery tags that are presented to the wrapped plain AMQP Consumer.
 *
 * <p>Ensures that message results (N)ACK/REJECT invoked by the consumer arrive at the correct
 * channel with the matching external delivery tag.
 */
public class MsgDeliveryTagMapping {

    private interface MsgResponse {
        Void apply(MsgResult msgResult, boolean multiple, boolean requeue, Predicate<Channel> when)
                throws IOException;
    }

    /**
     * MappingConsumer wraps a plain Consumer to support consuming messages from multiple channels
     * without conflicting deliveryTags.
     */
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
            envelope = envelopeWithPseudoDeliveryTag(channel, envelope);
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

    /**
     * MsgResult applies the result of the message processing to the channels by either (N)ACKing or
     * REJECTing the message.
     */
    private enum MsgResult {
        /**
         * ACK the message.
         *
         * @param channel channel
         * @param deliveryTag deliveryTag of message in channel
         * @param multiple apply to all messages up to the given delivery tag
         * @param requeue not applicable for ACKs
         * @throws IOException
         */
        ACK {
            @Override
            Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
                    throws IOException {
                channel.basicAck(deliveryTag, multiple);
                return null;
            }
        },
        /**
         * Performs an NACK for the message(s) on the given channel
         */
        NACK {
            /**
             * NACK the message.
             *
             * @param channel channel
             * @param deliveryTag deliveryTag of message in channel
             * @param multiple apply to all messages up to the given delivery tag
             * @param requeue requeue message if true
             * @throws IOException
             */
            @Override
            Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
                    throws IOException {
                channel.basicNack(deliveryTag, multiple, requeue);
                return null;
            }
        },
        /**
         * Rejects the message on the given channel
         */
        REJECT {
            /**
             * Reject the message.
             *
             * @param channel channel
             * @param deliveryTag deliveryTag of message in channel
             * @param multiple not applicable for REJECT
             * @param requeue requeue message if true
             * @throws IOException
             */
            @Override
            Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
                    throws IOException {
                channel.basicReject(deliveryTag, requeue);
                return null;
            }
        };

        /**
         * apply the result of processing the message on the given channel.
         *
         * @param channel     channel
         * @param deliveryTag deliveryTag of message in channel
         * @param multiple    true, if operation applies to multiple messages
         * @param requeue     requeue message if true
         * @throws IOException if the channel operation fails
         */
        abstract Void invoke(Channel channel, long deliveryTag, boolean multiple, boolean requeue)
                throws IOException;
    }

    private final AtomicLong deliveryTagGenerator;
    private final ConcurrentNavigableMap<Long, MsgResponse> deliveryTags;

    /**
     * Initializes the delivery tag mapper
     */
    MsgDeliveryTagMapping() {
        this.deliveryTagGenerator = new AtomicLong();
        this.deliveryTags = new ConcurrentSkipListMap<>();
    }

    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        Map<Long, MsgResponse> acks =
                multiple
                        ? deliveryTags.headMap(deliveryTag, true).descendingMap()
                        : deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        eachChannelOnce(acks, MsgResult.ACK, multiple, false);
        acks.clear();
    }

    void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        Map<Long, MsgResponse> nacks =
                multiple
                        ? deliveryTags.headMap(deliveryTag, true).descendingMap()
                        : deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        eachChannelOnce(nacks, MsgResult.NACK, multiple, requeue);
        nacks.clear();
    }

    void basicReject(long deliveryTag, boolean requeue) throws IOException {
        Map<Long, MsgResponse> rejections = deliveryTags.subMap(deliveryTag, deliveryTag + 1);
        eachChannelOnce(rejections, MsgResult.REJECT, false, requeue);
        rejections.clear();
    }

    Consumer createConsumerDecorator(Consumer delegate, Channel channel) {
        return new MappingConsumer(delegate, channel);
    }

    private void eachChannelOnce(
            Map<Long, MsgResponse> responseMap, MsgResult msgResult, boolean multiple, boolean requeue)
            throws IOException {

        if (responseMap.isEmpty()) {
            throw new IOException("Unknown delivery tag");
        }

        Set<Channel> alreadyUsed = new HashSet<>();
        responseMap.forEach(
                (BiConsumer<Long, MsgResponse>)
                        (tag, msgResponse) -> {
                            msgResponse.apply(msgResult, multiple, requeue, alreadyUsed::add);
                        });
    }

    /**
     * maps the actual delivery tag of the message broker to a synthetic to prevent collisions when
     * consuming from multiplexed channels
     *
     * @param channel     AMQP channel
     * @param deliveryTag real delivery tag
     * @return synthetic local delivery tag
     */
    private long mapDelivery(Channel channel, long deliveryTag) {
        long tag = deliveryTagGenerator.incrementAndGet();
        MsgResponse msgResponse = (msgResult, multiple, requeue, when) ->
                when.test(channel) ? msgResult.invoke(channel, deliveryTag, multiple, requeue) : null;
        deliveryTags.put(tag, msgResponse);
        return tag;
    }

    /**
     * Replace real message envelope with a local copy using the replaced synthetic delivery tag
     * internally.
     *
     * @param channel  AMQP channel
     * @param envelope real message envelope
     * @return local envelope
     */
    Envelope envelopeWithPseudoDeliveryTag(Channel channel, Envelope envelope) {
        long tag = mapDelivery(channel, envelope.getDeliveryTag());
        return new Envelope(
                tag, envelope.isRedeliver(), envelope.getExchange(), envelope.getRoutingKey());
    }

    /**
     * Replace real AMQP GetResponse with a local one using the synthetic delivery tag.
     *
     * @param channel  AMQP channel
     * @param response real GetResponse
     * @return local response
     * @see GetResponse
     */
    GetResponse responseWithPseudoDeliveryTag(Channel channel, GetResponse response) {
        if (response != null) {
            Envelope envelope = envelopeWithPseudoDeliveryTag(channel, response.getEnvelope());
            return new GetResponse(
                    envelope, response.getProps(), response.getBody(), response.getMessageCount());
        } else {
            return null;
        }
    }
}
