package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.logging.Level;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Basic;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Confirm.SelectOk;
import com.rabbitmq.client.AMQP.Exchange;
import com.rabbitmq.client.AMQP.Queue;
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
import com.xing.beetle.util.ExceptionSupport;

public interface DefaultChannel extends Channel {

  /**
   * Decorator provides default implementations of the (Default)Channel interface delegating to the
   * underlying channels.
   */
  interface Decorator extends DefaultChannel {

    /** Type specifies the type of connection */
    enum Type {
      /** Consumer connection */
      CONSUME,
      /** Publisher */
      PUBLISH,
      /** Synchronos RPC channel */
      RPC,
      /** State */
      STATE,
      /** Topology */
      TOPOLOGY;
    }

    @Override
    default void abort(int closeCode, String closeMessage) throws IOException {
      delegateForEach(Type.STATE, ch -> ch.abort(closeCode, closeMessage));
    }

    @Override
    default void addConfirmListener(ConfirmListener listener) {
      delegateForEach(Type.PUBLISH, ch -> ch.addConfirmListener(listener));
    }

    @Override
    default void addReturnListener(ReturnListener listener) {
      delegateForEach(Type.PUBLISH, ch -> ch.addReturnListener(listener));
    }

    @Override
    default void addShutdownListener(ShutdownListener listener) {
      delegateForEach(Type.STATE, ch -> ch.addShutdownListener(listener));
    }

    @Override
    default CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
      return delegateMap(Type.RPC, ch -> ch.asyncCompletableRpc(method));
    }

    @Override
    default void asyncRpc(Method method) throws IOException {
      delegateForEach(Type.RPC, ch -> ch.asyncRpc(method));
    }

    @Override
    default void basicAck(long deliveryTag, boolean multiple) throws IOException {
      delegateForEach(Type.CONSUME, ch -> ch.basicAck(deliveryTag, multiple));
    }

    @Override
    default void basicCancel(String consumerTag) throws IOException {
      delegateForEach(Type.CONSUME, ch -> ch.basicCancel(consumerTag));
    }

    @Override
    default String basicConsume(
        String queue,
        boolean autoAck,
        String consumerTag,
        boolean noLocal,
        boolean exclusive,
        Map<String, Object> arguments,
        Consumer callback)
        throws IOException {
      return delegateMap(
          Type.CONSUME,
          ch ->
              ch.basicConsume(
                  queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback));
    }

    @Override
    default GetResponse basicGet(String queue, boolean autoAck) throws IOException {
      return delegateMap(Type.CONSUME, ch -> ch.basicGet(queue, autoAck));
    }

    @Override
    default void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
      delegateForEach(Type.CONSUME, ch -> ch.basicNack(deliveryTag, multiple, requeue));
    }

    @Override
    default void basicPublish(
        String exchange,
        String routingKey,
        boolean mandatory,
        boolean immediate,
        BasicProperties props,
        byte[] body)
        throws IOException {
      delegateForEach(
          Type.PUBLISH,
          ch -> ch.basicPublish(exchange, routingKey, mandatory, immediate, props, body));
    }

    @Override
    default void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
      delegateForEach(Type.CONSUME, ch -> ch.basicQos(prefetchSize, prefetchCount, global));
    }

    @Override
    default Basic.RecoverOk basicRecover(boolean requeue) throws IOException {
      return delegateMap(Type.CONSUME, ch -> ch.basicRecover(requeue));
    }

    @Override
    default void basicReject(long deliveryTag, boolean requeue) throws IOException {
      delegateForEach(Type.CONSUME, ch -> ch.basicReject(deliveryTag, requeue));
    }

    @Override
    default void clearConfirmListeners() {
      delegateForEach(Type.PUBLISH, Channel::clearConfirmListeners);
    }

    @Override
    default void clearReturnListeners() {
      delegateForEach(Type.PUBLISH, Channel::clearReturnListeners);
    }

    @Override
    default void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
      delegateForEach(Type.STATE, ch -> ch.close(closeCode, closeMessage));
    }

    @Override
    default SelectOk confirmSelect() throws IOException {
      return delegateMap(Type.PUBLISH, Channel::confirmSelect);
    }

    @Override
    default long consumerCount(String queue) throws IOException {
      return delegateMap(Type.STATE, ch -> ch.consumerCount(queue));
    }

    default void delegateForEach(Type type, ExceptionSupport.Consumer<Channel> fn) {
      delegateMap(
          type,
          channel -> {
            fn.accept(channel);
            return null;
          });
    }

    <R> R delegateMap(Type type, ExceptionSupport.Function<Channel, ? extends R> fn);

    @Override
    default Exchange.BindOk exchangeBind(
        String destination, String source, String routingKey, Map<String, Object> arguments)
        throws IOException {
      return delegateMap(
          Type.TOPOLOGY, ch -> ch.exchangeBind(destination, source, routingKey, arguments));
    }

    @Override
    default void exchangeBindNoWait(
        String destination, String source, String routingKey, Map<String, Object> arguments)
        throws IOException {
      exchangeBind(destination, source, routingKey, arguments);
    }

    @Override
    default Exchange.DeclareOk exchangeDeclare(
        String exchange,
        String type,
        boolean durable,
        boolean autoDelete,
        boolean internal,
        Map<String, Object> arguments)
        throws IOException {
      return delegateMap(
          Type.TOPOLOGY,
          ch -> ch.exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments));
    }

    @Override
    default void exchangeDeclareNoWait(
        String exchange,
        String type,
        boolean durable,
        boolean autoDelete,
        boolean internal,
        Map<String, Object> arguments)
        throws IOException {
      exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
    }

    @Override
    default Exchange.DeclareOk exchangeDeclarePassive(String exchange) throws IOException {
      return delegateMap(Type.TOPOLOGY, ch -> ch.exchangeDeclarePassive(exchange));
    }

    @Override
    default Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
      return delegateMap(Type.TOPOLOGY, ch -> ch.exchangeDelete(exchange, ifUnused));
    }

    @Override
    default void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
      exchangeDelete(exchange, ifUnused);
    }

    @Override
    default Exchange.UnbindOk exchangeUnbind(
        String destination, String source, String routingKey, Map<String, Object> arguments)
        throws IOException {
      return delegateMap(
          Type.TOPOLOGY, ch -> ch.exchangeUnbind(destination, source, routingKey, arguments));
    }

    @Override
    default void exchangeUnbindNoWait(
        String destination, String source, String routingKey, Map<String, Object> arguments)
        throws IOException {
      exchangeUnbind(destination, source, routingKey, arguments);
    }

    @Override
    default int getChannelNumber() {
      return delegateMap(Type.STATE, Channel::getChannelNumber);
    }

    @Override
    default ShutdownSignalException getCloseReason() {
      return delegateMap(Type.STATE, Channel::getCloseReason);
    }

    @Override
    default Connection getConnection() {
      return delegateMap(Type.STATE, Channel::getConnection);
    }

    @Override
    default Consumer getDefaultConsumer() {
      return delegateMap(Type.CONSUME, Channel::getDefaultConsumer);
    }

    @Override
    default long getNextPublishSeqNo() {
      return delegateMap(Type.PUBLISH, Channel::getNextPublishSeqNo);
    }

    @Override
    default boolean isOpen() {
      return delegateMap(Type.STATE, Channel::isOpen);
    }

    @Override
    default void notifyListeners() {
      delegateForEach(Type.CONSUME, Channel::notifyListeners);
    }

    @Override
    default Queue.BindOk queueBind(
        String queue, String exchange, String routingKey, Map<String, Object> arguments)
        throws IOException {
      return delegateMap(Type.TOPOLOGY, ch -> ch.queueBind(queue, exchange, routingKey, arguments));
    }

    @Override
    default void queueBindNoWait(
        String queue, String exchange, String routingKey, Map<String, Object> arguments)
        throws IOException {
      queueBind(queue, exchange, routingKey, arguments);
    }

    @Override
    default Queue.DeclareOk queueDeclare(
        String queue,
        boolean durable,
        boolean exclusive,
        boolean autoDelete,
        Map<String, Object> arguments)
        throws IOException {
      return delegateMap(
          Type.TOPOLOGY, ch -> ch.queueDeclare(queue, durable, exclusive, autoDelete, arguments));
    }

    @Override
    default void queueDeclareNoWait(
        String queue,
        boolean durable,
        boolean exclusive,
        boolean autoDelete,
        Map<String, Object> arguments)
        throws IOException {
      queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }

    @Override
    default Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
      return delegateMap(Type.TOPOLOGY, ch -> ch.queueDeclarePassive(queue));
    }

    @Override
    default Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty)
        throws IOException {
      return delegateMap(Type.TOPOLOGY, ch -> ch.queueDelete(queue, ifUnused, ifEmpty));
    }

    @Override
    default void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty)
        throws IOException {
      queueDelete(queue, ifUnused, ifEmpty);
    }

    @Override
    default Queue.PurgeOk queuePurge(String queue) throws IOException {
      return delegateMap(Type.CONSUME, ch -> ch.queuePurge(queue));
    }

    @Override
    default Queue.UnbindOk queueUnbind(
        String queue, String exchange, String routingKey, Map<String, Object> arguments)
        throws IOException {
      return delegateMap(
          Type.TOPOLOGY, ch -> ch.queueUnbind(queue, exchange, routingKey, arguments));
    }

    @Override
    default boolean removeConfirmListener(ConfirmListener listener) {
      return delegateMap(Type.PUBLISH, ch -> ch.removeConfirmListener(listener));
    }

    @Override
    default boolean removeReturnListener(ReturnListener listener) {
      return delegateMap(Type.PUBLISH, ch -> ch.removeReturnListener(listener));
    }

    @Override
    default void removeShutdownListener(ShutdownListener listener) {
      delegateForEach(Type.STATE, ch -> ch.removeShutdownListener(listener));
    }

    @Override
    default Command rpc(Method method) throws IOException {
      return delegateMap(Type.RPC, ch -> ch.rpc(method));
    }

    @Override
    default void setDefaultConsumer(Consumer consumer) {
      delegateForEach(Type.CONSUME, ch -> ch.setDefaultConsumer(consumer));
    }

    @Override
    default Tx.CommitOk txCommit() throws IOException {
      return delegateMap(Type.RPC, Channel::txCommit);
    }

    @Override
    default Tx.RollbackOk txRollback() throws IOException {
      return delegateMap(Type.RPC, Channel::txRollback);
    }

    @Override
    default Tx.SelectOk txSelect() throws IOException {
      return delegateMap(Type.RPC, Channel::txSelect);
    }

    @Override
    default boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
      return delegateMap(Type.PUBLISH, ch -> ch.waitForConfirms(timeout));
    }
  }

  static Consumer consumerOf(
      DeliverCallback deliver,
      CancelCallback cancel,
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
        Logger.getLogger(getClass().getName())
            .log(Level.FINER, "%s: Channel closed\n", consumerTag);
      }

      @Override
      public void handleConsumeOk(String consumerTag) {
        Logger.getLogger(getClass().getName()).log(Level.FINER, "%s: ConsumeOK\n", consumerTag);
      }

      @Override
      public void handleDelivery(
          String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
          throws IOException {
        if (deliver != null) {
          deliver.handle(consumerTag, new Delivery(envelope, properties, body));
        }
      }

      @Override
      public void handleRecoverOk(String consumerTag) {
        Logger.getLogger(getClass().getName()).log(Level.FINER, "%s: RecoverOK", consumerTag);
      }

      @Override
      public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        if (shutdownSignal != null) {
          shutdownSignal.handleShutdownSignal(consumerTag, sig);
        }
      }
    };
  }

  static Consumer mappingConsumer(
      Consumer delegate, Channel channel, MsgDeliveryTagMapping tagMapping) {
    return new Consumer() {

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
          String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
          throws IOException {
        envelope = tagMapping.envelopeWithPseudoDeliveryTag(channel, envelope);
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
    };
  }

  @Override
  default void abort() throws IOException {
    Logger.getLogger(getClass().getName()).log(Level.FINER, "aborted");
    abort(AMQP.REPLY_SUCCESS, "OK");
  }

  @Override
  default ConfirmListener addConfirmListener(
      ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
    requireNonNull(ackCallback);
    requireNonNull(nackCallback);
    ConfirmListener listener =
        new ConfirmListener() {

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
        (replyCode, replyText, exchange, routingKey, properties, body) ->
            returnCallback.handle(
                new Return(replyCode, replyText, exchange, routingKey, properties, body));
    addReturnListener(listener);
    return listener;
  }

  @Override
  default String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException {
    return basicConsume(queue, autoAck, "", callback);
  }

  @Override
  default String basicConsume(
      String queue, boolean autoAck, DeliverCallback deliver, CancelCallback cancel)
      throws IOException {
    return basicConsume(queue, autoAck, "", consumerOf(deliver, cancel, null));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      DeliverCallback deliver,
      CancelCallback cancel,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(queue, autoAck, "", consumerOf(deliver, cancel, shutdownSignal));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      DeliverCallback deliver,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(queue, autoAck, "", consumerOf(deliver, null, shutdownSignal));
  }

  @Override
  default String basicConsume(
      String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback)
      throws IOException {
    return basicConsume(queue, autoAck, "", false, false, arguments, callback);
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      Map<String, Object> arguments,
      DeliverCallback deliver,
      CancelCallback cancel)
      throws IOException {
    return basicConsume(
        queue, autoAck, "", false, false, arguments, consumerOf(deliver, cancel, null));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      Map<String, Object> arguments,
      DeliverCallback deliver,
      CancelCallback cancel,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(
        queue, autoAck, "", false, false, arguments, consumerOf(deliver, cancel, shutdownSignal));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      Map<String, Object> arguments,
      DeliverCallback deliver,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(
        queue, autoAck, "", false, false, arguments, consumerOf(deliver, null, shutdownSignal));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      String consumerTag,
      boolean noLocal,
      boolean exclusive,
      Map<String, Object> arguments,
      DeliverCallback deliver,
      CancelCallback cancel)
      throws IOException {
    return basicConsume(
        queue,
        autoAck,
        consumerTag,
        noLocal,
        exclusive,
        arguments,
        consumerOf(deliver, cancel, null));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      String consumerTag,
      boolean noLocal,
      boolean exclusive,
      Map<String, Object> arguments,
      DeliverCallback deliver,
      CancelCallback cancel,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(
        queue,
        autoAck,
        consumerTag,
        noLocal,
        exclusive,
        arguments,
        consumerOf(deliver, cancel, shutdownSignal));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      String consumerTag,
      boolean noLocal,
      boolean exclusive,
      Map<String, Object> arguments,
      DeliverCallback deliver,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(
        queue,
        autoAck,
        consumerTag,
        noLocal,
        exclusive,
        arguments,
        consumerOf(deliver, null, shutdownSignal));
  }

  @Override
  default String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback)
      throws IOException {
    return basicConsume(queue, autoAck, consumerTag, false, false, null, callback);
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      String consumerTag,
      DeliverCallback deliver,
      CancelCallback cancel)
      throws IOException {
    return basicConsume(
        queue, autoAck, consumerTag, false, false, null, consumerOf(deliver, cancel, null));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      String consumerTag,
      DeliverCallback deliver,
      CancelCallback cancel,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(
        queue,
        autoAck,
        consumerTag,
        false,
        false,
        null,
        consumerOf(deliver, cancel, shutdownSignal));
  }

  @Override
  default String basicConsume(
      String queue,
      boolean autoAck,
      String consumerTag,
      DeliverCallback deliver,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(
        queue, autoAck, consumerTag, false, false, null, consumerOf(deliver, null, shutdownSignal));
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
  default String basicConsume(
      String queue,
      DeliverCallback deliver,
      CancelCallback cancel,
      ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(queue, consumerOf(deliver, cancel, shutdownSignal));
  }

  @Override
  default String basicConsume(
      String queue, DeliverCallback deliver, ConsumerShutdownSignalCallback shutdownSignal)
      throws IOException {
    return basicConsume(queue, consumerOf(deliver, null, shutdownSignal));
  }

  @Override
  default void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body)
      throws IOException {
    basicPublish(exchange, routingKey, false, props, body);
  }

  @Override
  default void basicPublish(
      String exchange, String routingKey, boolean mandatory, BasicProperties props, byte[] body)
      throws IOException {
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
  default Basic.RecoverOk basicRecover() throws IOException {
    return basicRecover(true);
  }

  @Override
  default void close() throws IOException, TimeoutException {
    Logger.getLogger(getClass().getName()).log(Level.FINER, "Channel close");
    close(AMQP.REPLY_SUCCESS, "OK");
  }

  @Override
  default Exchange.BindOk exchangeBind(String destination, String source, String routingKey)
      throws IOException {
    return exchangeBind(destination, source, routingKey, null);
  }

  @Override
  default Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type)
      throws IOException {
    return exchangeDeclare(exchange, type.getType());
  }

  @Override
  default Exchange.DeclareOk exchangeDeclare(
      String exchange, BuiltinExchangeType type, boolean durable) throws IOException {
    return exchangeDeclare(exchange, type.getType(), durable);
  }

  @Override
  default Exchange.DeclareOk exchangeDeclare(
      String exchange,
      BuiltinExchangeType type,
      boolean durable,
      boolean autoDelete,
      boolean internal,
      Map<String, Object> arguments)
      throws IOException {
    return exchangeDeclare(exchange, type.getType(), durable, autoDelete, internal, arguments);
  }

  @Override
  default Exchange.DeclareOk exchangeDeclare(
      String exchange,
      BuiltinExchangeType type,
      boolean durable,
      boolean autoDelete,
      Map<String, Object> arguments)
      throws IOException {
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
  default Exchange.DeclareOk exchangeDeclare(
      String exchange,
      String type,
      boolean durable,
      boolean autoDelete,
      Map<String, Object> arguments)
      throws IOException {
    return exchangeDeclare(exchange, type, durable, autoDelete, false, arguments);
  }

  @Override
  default void exchangeDeclareNoWait(
      String exchange,
      BuiltinExchangeType type,
      boolean durable,
      boolean autoDelete,
      boolean internal,
      Map<String, Object> arguments)
      throws IOException {
    exchangeDeclareNoWait(exchange, type.getType(), durable, autoDelete, internal, arguments);
  }

  @Override
  default Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
    return exchangeDelete(exchange, false);
  }

  @Override
  default Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey)
      throws IOException {
    return exchangeUnbind(destination, source, routingKey, null);
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
  default Queue.DeclareOk queueDeclare() throws IOException {
    return queueDeclare("", false, true, true, null);
  }

  @Override
  default Queue.DeleteOk queueDelete(String queue) throws IOException {
    return queueDelete(queue, false, false);
  }

  @Override
  default Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey)
      throws IOException {
    return queueUnbind(queue, exchange, routingKey, null);
  }

  @Override
  default boolean waitForConfirms() throws InterruptedException {
    boolean confirms = false;
    try {
      confirms = waitForConfirms(0L);
    } catch (TimeoutException e) {
      // ignored
    }
    return confirms;
  }

  @Override
  default void waitForConfirmsOrDie() throws IOException, InterruptedException {
    try {
      waitForConfirmsOrDie(0L);
    } catch (TimeoutException e) {
      // ignored
    }
  }

  @Override
  default void waitForConfirmsOrDie(long timeout)
      throws IOException, InterruptedException, TimeoutException {
    try {
      if (!waitForConfirms(timeout)) {
        Logger.getLogger(getClass().getName()).log(Level.FINER, "Close because of NACKS");
        close(AMQP.REPLY_SUCCESS, "NACKS RECEIVED");
        throw new IOException("nacks received");
      }
    } catch (TimeoutException e) {
      close(AMQP.PRECONDITION_FAILED, "TIMEOUT WAITING FOR ACK");
      throw (e);
    }
  }
}
