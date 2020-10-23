package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;
import com.xing.beetle.util.ExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DefaultConnection extends the basic AMQP connection interface. */
public interface DefaultConnection extends Connection {

  Logger log = LoggerFactory.getLogger(DefaultConnection.class);
  /**
   * Decorator provides default implementations for one or more AMQP connections and delegates calls
   * where appropriate.
   */
  interface Decorator extends DefaultConnection {

    @Override
    default void abort(int closeCode, String closeMessage, int timeout) {
      delegateForEach(con -> con.abort(closeCode, closeMessage, timeout));
    }

    @Override
    default void addBlockedListener(BlockedListener listener) {
      delegateForEach(con -> con.addBlockedListener(listener));
    }

    @Override
    default void addShutdownListener(ShutdownListener listener) {
      delegateForEach(con -> con.addShutdownListener(listener));
    }

    @Override
    default void clearBlockedListeners() {
      delegateForEach(Connection::clearBlockedListeners);
    }

    @Override
    default void close(int closeCode, String closeMessage, int timeout) throws IOException {
      delegateForEach(con -> con.close(closeCode, closeMessage, timeout));
    }

    default void delegateForEach(ExceptionSupport.Consumer<Connection> fn) {
      delegateMap(
          connection -> {
            fn.accept(connection);
            return null;
          });
    }

    <R> R delegateMap(ExceptionSupport.Function<Connection, ? extends R> fn);

    @Override
    default InetAddress getAddress() {
      return delegateMap(Connection::getAddress);
    }

    @Override
    default int getChannelMax() {
      return delegateMap(Connection::getChannelMax);
    }

    @Override
    default Map<String, Object> getClientProperties() {
      return delegateMap(Connection::getClientProperties);
    }

    @Override
    default String getClientProvidedName() {
      return delegateMap(Connection::getClientProvidedName);
    }

    @Override
    default ShutdownSignalException getCloseReason() {
      return delegateMap(Connection::getCloseReason);
    }

    @Override
    default ExceptionHandler getExceptionHandler() {
      return delegateMap(Connection::getExceptionHandler);
    }

    @Override
    default int getFrameMax() {
      return delegateMap(Connection::getFrameMax);
    }

    @Override
    default int getHeartbeat() {
      return delegateMap(Connection::getHeartbeat);
    }

    @Override
    default String getId() {
      return delegateMap(Connection::getId);
    }

    @Override
    default int getPort() {
      return delegateMap(Connection::getPort);
    }

    @Override
    default Map<String, Object> getServerProperties() {
      return delegateMap(Connection::getServerProperties);
    }

    @Override
    default boolean isOpen() {
      return delegateMap(Connection::isOpen);
    }

    @Override
    default void notifyListeners() {
      delegateForEach(Connection::notifyListeners);
    }

    @Override
    default boolean removeBlockedListener(BlockedListener listener) {
      return delegateMap(con -> con.removeBlockedListener(listener));
    }

    @Override
    default void removeShutdownListener(ShutdownListener listener) {
      delegateForEach(con -> con.removeShutdownListener(listener));
    }

    @Override
    default void setId(String id) {
      delegateForEach(con -> con.setId(id));
    }
  }

  @Override
  default void abort() {
    abort(-1);
  }

  @Override
  default void abort(int timeout) {
    log.debug("abort w/timeout");
    abort(AMQP.REPLY_SUCCESS, "OK", timeout);
  }

  @Override
  default void abort(int closeCode, String closeMessage) {
    abort(closeCode, closeMessage, -1);
  }

  default BlockedListener addBlockedListener(
      BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
    requireNonNull(blockedCallback);
    requireNonNull(unblockedCallback);
    BlockedListener listener =
        new BlockedListener() {

          @Override
          public void handleBlocked(String reason) throws IOException {
            blockedCallback.handle(reason);
          }

          @Override
          public void handleUnblocked() throws IOException {
            unblockedCallback.handle();
          }
        };
    addBlockedListener(listener);
    return listener;
  }

  @Override
  default void close() throws IOException {
    close(-1);
  }

  @Override
  default void close(int timeout) throws IOException {
    log.debug("close");
    close(AMQP.REPLY_SUCCESS, "OK", timeout);
  }

  @Override
  default void close(int closeCode, String closeMessage) throws IOException {
    close(closeCode, closeMessage, -1);
  }

  @Override
  default Channel createChannel() throws IOException {
    return createChannel(-1);
  }
}
