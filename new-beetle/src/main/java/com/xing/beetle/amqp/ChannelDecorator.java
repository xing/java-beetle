package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Basic;
import com.rabbitmq.client.AMQP.Basic.RecoverOk;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Confirm.SelectOk;
import com.rabbitmq.client.AMQP.Exchange;
import com.rabbitmq.client.AMQP.Exchange.BindOk;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.AMQP.Exchange.UnbindOk;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.AMQP.Queue.PurgeOk;
import com.rabbitmq.client.AMQP.Tx;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnexpectedMethodError;
import com.xing.beetle.util.ExceptionSupport;

public interface ChannelDecorator extends Channel {

    interface Async extends ChannelDecorator, com.xing.beetle.util.Async<Channel> {

        @Override
        default void abort(int closeCode, String closeMessage) throws IOException {
            performJoined(c -> c.abort(closeCode, closeMessage));
        }

        @Override
        default void addConfirmListener(ConfirmListener listener) {
            performJoined(c -> c.addConfirmListener(listener));
        }

        @Override
        default void addReturnListener(ReturnListener listener) {
            performJoined(c -> c.addReturnListener(listener));
        }

        @Override
        default void addShutdownListener(ShutdownListener listener) {
            performJoined(c -> c.addShutdownListener(listener));
        }

        @Override
        default CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
            return execute(c -> c.asyncCompletableRpc(method).join()).toCompletableFuture();
        }

        @Override
        default void asyncRpc(Method method) throws IOException {
            performJoined(c -> c.asyncRpc(method));
        }

        @Override
        default void basicCancel(String consumerTag) throws IOException {
            performJoined(c -> c.basicCancel(consumerTag));
        }

        @Override
        default String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
                                    boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
            return executeJoined(c -> c.basicConsume(queue, autoAck, consumerTag, noLocal, exclusive,
                    arguments, callback));
        }

        @Override
        default void basicPublish(String exchange, String routingKey, boolean mandatory,
                                  boolean immediate, BasicProperties props, byte[] body) throws IOException {
            performJoined(c -> c.basicPublish(exchange, routingKey, mandatory, immediate, props, body));
        }

        @Override
        default void clearConfirmListeners() {
            performJoined(Channel::clearConfirmListeners);
        }

        @Override
        default void clearReturnListeners() {
            performJoined(Channel::clearReturnListeners);
        }

        @Override
        default void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
            performJoined(c -> c.close(closeCode, closeMessage));
        }

        @Override
        default SelectOk confirmSelect() throws IOException {
            return executeJoined(Channel::confirmSelect);
        }

        @Override
        default long consumerCount(String queue) throws IOException {
            return executeJoined(c -> c.consumerCount(queue));
        }

        @Override
        default int getChannelNumber() {
            return executeJoined(Channel::getChannelNumber);
        }

        @Override
        default ShutdownSignalException getCloseReason() {
            return executeJoined(Channel::getCloseReason);
        }

        @Override
        default Connection getConnection() {
            return executeJoined(Channel::getConnection);
        }

        @Override
        default Consumer getDefaultConsumer() {
            return executeJoined(Channel::getDefaultConsumer);
        }

        @Override
        default long getNextPublishSeqNo() {
            return executeJoined(Channel::getNextPublishSeqNo);
        }

        @Override
        default boolean isOpen() {
            return executeJoined(Channel::isOpen);
        }

        @Override
        default void notifyListeners() {
            performJoined(Channel::notifyListeners);
        }

        @Override
        default boolean removeConfirmListener(ConfirmListener listener) {
            return executeJoined(c -> c.removeConfirmListener(listener));
        }

        @Override
        default boolean removeReturnListener(ReturnListener listener) {
            return executeJoined(c -> c.removeReturnListener(listener));
        }

        @Override
        default void removeShutdownListener(ShutdownListener listener) {
            performJoined(c -> c.removeShutdownListener(listener));
        }

        @Override
        default Command rpc(Method method) throws IOException {
            return executeJoined(c -> c.rpc(method));
        }

        @Override
        default void setDefaultConsumer(Consumer consumer) {
            performJoined(c -> c.setDefaultConsumer(consumer));
        }

        @Override
        default boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
            return executeJoined(c -> c.waitForConfirms(timeout));
        }
    }

    interface Multiple extends ChannelDecorator {

        class MappingConsumer implements Consumer {

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

        @Override
        default void abort(int closeCode, String closeMessage) throws IOException {
            delegateForEach(c -> c.abort(closeCode, closeMessage));
        }

        @Override
        default void addConfirmListener(ConfirmListener listener) {
            delegateForEach(c -> c.addConfirmListener(requireNonNull(listener)));
        }

        @Override
        default void addReturnListener(ReturnListener listener) {
            delegateForEach(c -> c.addReturnListener(requireNonNull(listener)));
        }

        @Override
        default void addShutdownListener(ShutdownListener listener) {
            delegateForEach(c -> c.addShutdownListener(requireNonNull(listener)));
        }

        @Override
        default CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
            return delegateMap(c -> c.asyncCompletableRpc(method)).reduce(
                    ChannelDecorator.failedFuture(new IllegalStateException("No broker specified")),
                    (f1, f2) -> f1.thenCombine(f2, (c1, c2) -> c2));
        }

        @Override
        default void asyncRpc(Method method) throws IOException {
            delegateForEach(c -> c.asyncRpc(method));
        }

        MsgDeliveryTagMapping deliveryTagMapping();

        @Override
        default void basicAck(long deliveryTag, boolean multiple) throws IOException {
            deliveryTagMapping().basicAck(deliveryTag, multiple);
        }

        @Override
        default GetResponse basicGet(String queue, boolean autoAck) throws IOException {
            class ChannelResponse {

                private Channel channel;
                private GetResponse response;

                public ChannelResponse(Channel channel, GetResponse response) {
                    this.channel = channel;
                    this.response = response;
                }
            }
            return delegateMap(c -> new ChannelResponse(c, c.basicGet(queue, false)))
                    .filter(cr -> cr.response != null)
                    .map(cr -> deliveryTagMapping().mapResponse(cr.channel, cr.response))
                    .findAny()
                    .orElse(null);
        }

        @Override
        default String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
            String tag =
                    consumerTag == null || consumerTag.isEmpty() ? UUID.randomUUID().toString() : consumerTag;
            boolean all = delegateMap(
                    c -> c.basicConsume(queue, autoAck, tag, noLocal, exclusive, arguments, new MappingConsumer(callback, c, deliveryTagMapping())))
                    .allMatch(tag::equals);
            if (!all) {
                throw new AssertionError("Returned consumer tags dont match");
            }
            return tag;
        }

        @Override
        default void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
            deliveryTagMapping().basicNack(deliveryTag, multiple, requeue);
        }

        @Override
        default void basicReject(long deliveryTag, boolean requeue) throws IOException {
            deliveryTagMapping().basicReject(deliveryTag, requeue);
        }

        @Override
        default void clearConfirmListeners() {
            delegateForEach(Channel::clearConfirmListeners);
        }

        @Override
        default void clearReturnListeners() {
            delegateForEach(Channel::clearReturnListeners);
        }

        @Override
        default void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
            delegateForEach(c -> c.close(closeCode, closeMessage));
        }

        @Override
        default SelectOk confirmSelect() throws IOException {
            return delegateMap(Channel::confirmSelect).reduce((r1, r2) -> r2).get();
        }

        @Override
        default long consumerCount(String queue) throws IOException {
            return delegateMap(c -> c.consumerCount(queue)).findFirst().orElse(0L);
        }

        default void delegateForEach(ExceptionSupport.Consumer<Channel> action) {
            delegates().forEach(action);
        }

        default <R> Stream<R> delegateMap(ExceptionSupport.Function<Channel, R> mapper) {
            return delegates().map(mapper);
        }

        Stream<? extends Channel> delegates();

        @Override
        default int getChannelNumber() {
            return delegateMap(Channel::getChannelNumber).findFirst().get();
        }

        @Override
        default ShutdownSignalException getCloseReason() {
            return delegateMap(Channel::getCloseReason).filter(Objects::nonNull).findAny().orElse(null);
        }

        @Override
        default Connection getConnection() {
            throw new UnsupportedOperationException();
        }

        @Override
        default Consumer getDefaultConsumer() {
            return delegateMap(Channel::getDefaultConsumer).findAny().get();
        }

        @Override
        default boolean isOpen() {
            return delegates().anyMatch(Channel::isOpen);
        }

        @Override
        default void notifyListeners() {
            delegateForEach(Channel::notifyListeners);
        }

        @Override
        default boolean removeConfirmListener(ConfirmListener listener) {
            return delegates().allMatch(c -> c.removeConfirmListener(listener));
        }

        @Override
        default boolean removeReturnListener(ReturnListener listener) {
            return delegates().allMatch(c -> c.removeReturnListener(listener));
        }

        @Override
        default void removeShutdownListener(ShutdownListener listener) {
            delegateForEach(c -> c.removeShutdownListener(listener));
        }

        @Override
        default Command rpc(Method method) throws IOException {
            return delegateMap(c -> c.rpc(method)).reduce((r1, r2) -> r2).get();
        }

        @Override
        default void setDefaultConsumer(Consumer consumer) {
            delegateForEach(c -> c.setDefaultConsumer(consumer));
        }

        @Override
        default boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
            return delegateMap(c -> c.waitForConfirms(timeout)).allMatch(Boolean.TRUE::equals);
        }
    }

    interface Single extends ChannelDecorator {

        @Override
        default void abort(int closeCode, String closeMessage) throws IOException {
            delegate().abort(closeCode, closeMessage);
        }

        @Override
        default void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) throws IOException {
            delegate().basicPublish(exchange, routingKey, mandatory, immediate, props, body);
        }

        @Override
        default String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
            return delegate().basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
        }

        @Override
        default void basicCancel(String consumerTag) throws IOException {
            delegate().basicCancel(consumerTag);
        }

        @Override
        default AMQP.Confirm.SelectOk confirmSelect() throws IOException {
            return delegate().confirmSelect();
        }

        @Override
        default void asyncRpc(Method method) throws IOException {
            delegate().asyncRpc(method);
        }

        @Override
        default Command rpc(Method method) throws IOException {
            return delegate().rpc(method);
        }

        @Override
        default CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
            return delegate().asyncCompletableRpc(method);
        }

        @Override
        default void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
            delegate().close(closeCode, closeMessage);
        }

        @Override
        default void addConfirmListener(ConfirmListener listener) {
            delegate().addConfirmListener(requireNonNull(listener));
        }

        @Override
        default void addReturnListener(ReturnListener listener) {
            delegate().addReturnListener(requireNonNull(listener));
        }

        @Override
        default void addShutdownListener(ShutdownListener listener) {
            delegate().addShutdownListener(requireNonNull(listener));
        }

        @Override
        default void clearConfirmListeners() {
            delegate().clearConfirmListeners();
        }

        @Override
        default void clearReturnListeners() {
            delegate().clearReturnListeners();
        }

        @Override
        default long consumerCount(String queue) throws IOException {
            return delegate().consumerCount(queue);
        }

        Channel delegate();

        @Override
        default int getChannelNumber() {
            return delegate().getChannelNumber();
        }

        @Override
        default ShutdownSignalException getCloseReason() {
            return delegate().getCloseReason();
        }

        @Override
        default Connection getConnection() {
            return delegate().getConnection();
        }

        @Override
        default Consumer getDefaultConsumer() {
            return delegate().getDefaultConsumer();
        }

        @Override
        default long getNextPublishSeqNo() {
            return delegate().getNextPublishSeqNo();
        }

        @Override
        default boolean isOpen() {
            return delegate().isOpen();
        }

        @Override
        default void notifyListeners() {
            delegate().notifyListeners();
        }

        @Override
        default boolean removeConfirmListener(ConfirmListener listener) {
            return delegate().removeConfirmListener(listener);
        }

        @Override
        default boolean removeReturnListener(ReturnListener listener) {
            return delegate().removeReturnListener(listener);
        }

        @Override
        default void removeShutdownListener(ShutdownListener listener) {
            delegate().removeShutdownListener(listener);
        }

        @Override
        default void setDefaultConsumer(Consumer consumer) {
            delegate().setDefaultConsumer(consumer);
        }

        @Override
        default boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
            return delegate().waitForConfirms(timeout);
        }
    }

    static Consumer consumerOf(DeliverCallback deliver, CancelCallback cancel,
                               ConsumerShutdownSignalCallback shutdownSignal) {
        return new Consumer() {

            @Override
            public void handleCancel(String consumerTag) throws IOException {
                if (cancel != null) {
                    cancel.handle(consumerTag);
                }
            }

            @Override
            public void handleCancelOk(String consumerTag) {
            }

            @Override
            public void handleConsumeOk(String consumerTag) {
            }

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                                       byte[] body) throws IOException {
                if (deliver != null) {
                    deliver.handle(consumerTag, new Delivery(envelope, properties, body));
                }
            }

            @Override
            public void handleRecoverOk(String consumerTag) {
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                if (shutdownSignal != null) {
                    shutdownSignal.handleShutdownSignal(consumerTag, sig);
                }
            }
        };
    }

    static <T> CompletableFuture<T> failedFuture(Exception exception) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(exception);
        return future;
    }

    @Override
    default void abort() throws IOException {
        abort(AMQP.REPLY_SUCCESS, "OK");
    }

    @Override
    default ConfirmListener addConfirmListener(ConfirmCallback ackCallback,
                                               ConfirmCallback nackCallback) {
        requireNonNull(ackCallback);
        requireNonNull(nackCallback);
        ConfirmListener listener = new ConfirmListener() {

            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                ackCallback.handle(deliveryTag, multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                nackCallback.handle(deliveryTag, multiple);
            }
        };
        addConfirmListener(listener);
        return listener;
    }

    @Override
    default ReturnListener addReturnListener(ReturnCallback returnCallback) {
        ReturnListener listener =
                (replyCode, replyText, exchange, routingKey, properties, body) -> returnCallback
                        .handle(new Return(replyCode, replyText, exchange, routingKey, properties, body));
        addReturnListener(listener);
        return listener;
    }

    @Override
    default void basicAck(long deliveryTag, boolean multiple) throws IOException {
        asyncRpc(new Basic.Ack.Builder().deliveryTag(deliveryTag).multiple(multiple).build());
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, "", callback);
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, DeliverCallback deliver,
                                CancelCallback cancel) throws IOException {
        return basicConsume(queue, autoAck, "", consumerOf(deliver, cancel, null));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, DeliverCallback deliver,
                                CancelCallback cancel, ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, autoAck, "", consumerOf(deliver, cancel, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, DeliverCallback deliver,
                                ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, autoAck, "", consumerOf(deliver, null, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
                                Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments, callback);
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
                                DeliverCallback deliver, CancelCallback cancel) throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments,
                consumerOf(deliver, cancel, null));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
                                DeliverCallback deliver, CancelCallback cancel, ConsumerShutdownSignalCallback shutdownSignal)
            throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments,
                consumerOf(deliver, cancel, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
                                DeliverCallback deliver, ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments,
                consumerOf(deliver, null, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
                                boolean exclusive, Map<String, Object> arguments, DeliverCallback deliver,
                                CancelCallback cancel) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments,
                consumerOf(deliver, cancel, null));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
                                boolean exclusive, Map<String, Object> arguments, DeliverCallback deliver,
                                CancelCallback cancel, ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments,
                consumerOf(deliver, cancel, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
                                boolean exclusive, Map<String, Object> arguments, DeliverCallback deliver,
                                ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments,
                consumerOf(deliver, null, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback)
            throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, null, callback);
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, String consumerTag,
                                DeliverCallback deliver, CancelCallback cancel) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, null,
                consumerOf(deliver, cancel, null));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, String consumerTag,
                                DeliverCallback deliver, CancelCallback cancel, ConsumerShutdownSignalCallback shutdownSignal)
            throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, null,
                consumerOf(deliver, cancel, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, boolean autoAck, String consumerTag,
                                DeliverCallback deliver, ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, null,
                consumerOf(deliver, null, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, Consumer callback) throws IOException {
        return basicConsume(queue, false, callback);
    }

    @Override
    default String basicConsume(String queue, DeliverCallback deliver, CancelCallback cancel)
            throws IOException {
        return basicConsume(queue, consumerOf(deliver, cancel, null));
    }

    @Override
    default String basicConsume(String queue, DeliverCallback deliver, CancelCallback cancel,
                                ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, consumerOf(deliver, cancel, shutdownSignal));
    }

    @Override
    default String basicConsume(String queue, DeliverCallback deliver,
                                ConsumerShutdownSignalCallback shutdownSignal) throws IOException {
        return basicConsume(queue, consumerOf(deliver, null, shutdownSignal));
    }

    @Override
    default GetResponse basicGet(String queue, boolean autoAck) throws IOException {
        Command command = rpc(new Basic.Get.Builder().queue(queue).noAck(autoAck).build());
        Method method = command.getMethod();
        if (method instanceof Basic.GetOk) {
            Basic.GetOk getOk = (Basic.GetOk) method;
            Envelope envelope = new Envelope(getOk.getDeliveryTag(), getOk.getRedelivered(),
                    getOk.getExchange(), getOk.getRoutingKey());
            BasicProperties props = (BasicProperties) command.getContentHeader();
            byte[] body = command.getContentBody();
            int messageCount = getOk.getMessageCount();
            return new GetResponse(envelope, props, body, messageCount);
        } else if (method instanceof Basic.GetEmpty) {
            return null;
        } else {
            throw new UnexpectedMethodError(method);
        }
    }

    @Override
    default void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        asyncRpc(new Basic.Nack.Builder().deliveryTag(deliveryTag).multiple(multiple).requeue(requeue)
                .build());
    }

    @Override
    default void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body)
            throws IOException {
        basicPublish(exchange, routingKey, false, props, body);
    }

    @Override
    default void basicPublish(String exchange, String routingKey, boolean mandatory,
                              BasicProperties props, byte[] body) throws IOException {
        basicPublish(exchange, routingKey, mandatory, false, props, body);
    }

    @Override
    default void basicQos(int prefetchCount) throws IOException {
        basicQos(0, prefetchCount, false);
    }

    @Override
    default void basicQos(int prefetchCount, boolean global) throws IOException {
        basicQos(0, prefetchCount, global);
    }

    @Override
    default void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
        rpc(new Basic.Qos.Builder().prefetchSize(prefetchSize).prefetchCount(prefetchCount)
                .global(global).build());
    }

    @Override
    default Basic.RecoverOk basicRecover() throws IOException {
        return basicRecover(true);
    }

    @Override
    default RecoverOk basicRecover(boolean requeue) throws IOException {
        return (Basic.RecoverOk) rpc(new Basic.Recover.Builder().requeue(requeue).build()).getMethod();
    }

    @Override
    default void basicReject(long deliveryTag, boolean requeue) throws IOException {
        asyncRpc(new Basic.Reject.Builder().deliveryTag(deliveryTag).requeue(requeue).build());
    }

    @Override
    default void close() throws IOException, TimeoutException {
        close(AMQP.REPLY_SUCCESS, "OK");
    }

    @Override
    default Exchange.BindOk exchangeBind(String destination, String source, String routingKey)
            throws IOException {
        return exchangeBind(destination, source, routingKey, null);
    }

    @Override
    default BindOk exchangeBind(String destination, String source, String routingKey,
                                Map<String, Object> arguments) throws IOException {
        return (Exchange.BindOk) rpc(new Exchange.Bind.Builder().destination(destination).source(source)
                .routingKey(routingKey).arguments(arguments).nowait(false).build()).getMethod();
    }

    @Override
    default void exchangeBindNoWait(String destination, String source, String routingKey,
                                    Map<String, Object> arguments) throws IOException {
        asyncRpc(new Exchange.Bind.Builder().destination(destination).source(source)
                .routingKey(routingKey).arguments(arguments).nowait(true).build());
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type)
            throws IOException {
        return exchangeDeclare(exchange, type.getType());
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type,
                                               boolean durable) throws IOException {
        return exchangeDeclare(exchange, type.getType(), durable);
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type,
                                               boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments)
            throws IOException {
        return exchangeDeclare(exchange, type.getType(), durable, autoDelete, internal, arguments);
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type,
                                               boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type.getType(), durable, autoDelete, arguments);
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
        return exchangeDeclare(exchange, type, false, false, null);
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable)
            throws IOException {
        return exchangeDeclare(exchange, type, durable, false, null);
    }

    @Override
    default DeclareOk exchangeDeclare(String exchange, String type, boolean durable,
                                      boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        return (Exchange.DeclareOk) rpc(new Exchange.Declare.Builder().exchange(exchange).type(type)
                .durable(durable).autoDelete(autoDelete).internal(internal).arguments(arguments)
                .passive(false).nowait(false).build()).getMethod();
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable,
                                               boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type, durable, autoDelete, false, arguments);
    }

    @Override
    default void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable,
                                       boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        exchangeDeclareNoWait(exchange, type.getType(), durable, autoDelete, internal, arguments);
    }

    @Override
    default void exchangeDeclareNoWait(String exchange, String type, boolean durable,
                                       boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        asyncRpc(new Exchange.Declare.Builder().exchange(exchange).type(type).durable(durable)
                .autoDelete(autoDelete).internal(internal).arguments(arguments).passive(false).nowait(true)
                .build());
    }

    @Override
    default DeclareOk exchangeDeclarePassive(String exchange) throws IOException {
        return (Exchange.DeclareOk) rpc(new Exchange.Declare.Builder().exchange(exchange).type("")
                .passive(true).nowait(false).build()).getMethod();
    }

    @Override
    default Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
        return exchangeDelete(exchange, false);
    }

    @Override
    default DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
        return (Exchange.DeleteOk) rpc(
                new Exchange.Delete.Builder().exchange(exchange).ifUnused(ifUnused).nowait(false).build())
                .getMethod();
    }

    @Override
    default void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
        asyncRpc(
                new Exchange.Delete.Builder().exchange(exchange).ifUnused(ifUnused).nowait(true).build());
    }

    @Override
    default Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey)
            throws IOException {
        return exchangeUnbind(destination, source, routingKey, null);
    }

    @Override
    default UnbindOk exchangeUnbind(String destination, String source, String routingKey,
                                    Map<String, Object> arguments) throws IOException {
        return (Exchange.UnbindOk) rpc(new Exchange.Unbind.Builder().destination(destination)
                .source(source).routingKey(routingKey).arguments(arguments).nowait(false).build())
                .getMethod();
    }

    @Override
    default void exchangeUnbindNoWait(String destination, String source, String routingKey,
                                      Map<String, Object> arguments) throws IOException {
        asyncRpc(new Exchange.Unbind.Builder().destination(destination).source(source)
                .routingKey(routingKey).arguments(arguments).nowait(true).build());
    }

    @Override
    default long messageCount(String queue) throws IOException {
        Queue.DeclareOk ok = queueDeclarePassive(queue);
        return ok.getMessageCount();
    }

    @Override
    default Queue.BindOk queueBind(String queue, String exchange, String routingKey)
            throws IOException {
        return queueBind(queue, exchange, routingKey, null);
    }

    @Override
    default Queue.BindOk queueBind(String queue, String exchange, String routingKey,
                                   Map<String, Object> arguments) throws IOException {
        return (Queue.BindOk) rpc(new Queue.Bind.Builder().queue(queue).exchange(exchange)
                .routingKey(routingKey).arguments(arguments).nowait(false).build()).getMethod();
    }

    @Override
    default void queueBindNoWait(String queue, String exchange, String routingKey,
                                 Map<String, Object> arguments) throws IOException {
        asyncRpc(new Queue.Bind.Builder().queue(queue).exchange(exchange).routingKey(routingKey)
                .arguments(arguments).nowait(true).build());
    }

    @Override
    default Queue.DeclareOk queueDeclare() throws IOException {
        return queueDeclare("", false, true, true, null);
    }

    @Override
    default Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive,
                                         boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return (Queue.DeclareOk) rpc(
                new Queue.Declare.Builder().queue(queue).durable(durable).exclusive(exclusive)
                        .autoDelete(autoDelete).arguments(arguments).passive(false).nowait(false).build())
                .getMethod();
    }

    @Override
    default void queueDeclareNoWait(String queue, boolean durable, boolean exclusive,
                                    boolean autoDelete, Map<String, Object> arguments) throws IOException {
        asyncRpc(new Queue.Declare.Builder().queue(queue).durable(durable).exclusive(exclusive)
                .autoDelete(autoDelete).arguments(arguments).passive(false).nowait(true).build());
    }

    @Override
    default Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
        return (Queue.DeclareOk) rpc(new Queue.Declare.Builder().queue(queue).durable(false)
                .exclusive(true).autoDelete(true).passive(true).build()).getMethod();
    }

    @Override
    default Queue.DeleteOk queueDelete(String queue) throws IOException {
        return queueDelete(queue, false, false);
    }

    @Override
    default Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty)
            throws IOException {
        return (Queue.DeleteOk) rpc(new Queue.Delete.Builder().queue(queue).ifUnused(ifUnused)
                .ifEmpty(ifEmpty).nowait(false).build()).getMethod();
    }

    @Override
    default void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty)
            throws IOException {
        asyncRpc(new Queue.Delete.Builder().queue(queue).ifUnused(ifUnused).ifEmpty(ifEmpty)
                .nowait(true).build());
    }

    @Override
    default PurgeOk queuePurge(String queue) throws IOException {
        return (Queue.PurgeOk) rpc(new Queue.Purge.Builder().queue(queue).build()).getMethod();
    }

    @Override
    default Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey)
            throws IOException {
        return queueUnbind(queue, exchange, routingKey, null);
    }

    @Override
    default Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey,
                                       Map<String, Object> arguments) throws IOException {
        return (Queue.UnbindOk) rpc(new Queue.Unbind.Builder().queue(queue).exchange(exchange)
                .routingKey(routingKey).arguments(arguments).build()).getMethod();
    }

    @Override
    default Tx.CommitOk txCommit() throws IOException {
        return (Tx.CommitOk) rpc(new Tx.Commit.Builder().build()).getMethod();
    }

    @Override
    default Tx.RollbackOk txRollback() throws IOException {
        return (Tx.RollbackOk) rpc(new Tx.Rollback.Builder().build()).getMethod();
    }

    @Override
    default Tx.SelectOk txSelect() throws IOException {
        return (Tx.SelectOk) rpc(new Tx.Select.Builder().build()).getMethod();
    }

    @Override
    default boolean waitForConfirms() throws InterruptedException {
        boolean confirms = false;
        try {
            confirms = waitForConfirms(0L);
        } catch (TimeoutException e) {
        }
        return confirms;
    }

    @Override
    default void waitForConfirmsOrDie() throws IOException, InterruptedException {
        try {
            waitForConfirmsOrDie(0L);
        } catch (TimeoutException e) {
        }
    }

    @Override
    default void waitForConfirmsOrDie(long timeout)
            throws IOException, InterruptedException, TimeoutException {
        try {
            if (!waitForConfirms(timeout)) {
                close(AMQP.REPLY_SUCCESS, "NACKS RECEIVED");
                throw new IOException("nacks received");
            }
        } catch (TimeoutException e) {
            close(AMQP.PRECONDITION_FAILED, "TIMEOUT WAITING FOR ACK");
            throw (e);
        }
    }
}
