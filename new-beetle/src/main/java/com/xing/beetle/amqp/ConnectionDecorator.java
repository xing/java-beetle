package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;
import com.xing.beetle.util.ExceptionSupport.Consumer;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public interface ConnectionDecorator extends Connection {

  interface Async extends ConnectionDecorator, com.xing.beetle.util.Async<Connection> {

    @Override
    default void abort(int closeCode, String closeMessage, int timeout) {
      performJoined(c -> c.abort(closeCode, closeMessage, timeout));
    }

    @Override
    default void addBlockedListener(BlockedListener listener) {
      perform(c -> c.addBlockedListener(listener));
    }

    @Override
    default void addShutdownListener(ShutdownListener listener) {
      perform(c -> c.addShutdownListener(listener));
    }

    @Override
    default void clearBlockedListeners() {
      perform(c -> c.clearBlockedListeners());
    }

    @Override
    default void close(int closeCode, String closeMessage, int timeout) throws IOException {
      performJoined(c -> c.close(closeCode, closeMessage, timeout));
    }

    @Override
    default InetAddress getAddress() {
      return executeJoined(Connection::getAddress);
    }

    @Override
    default int getChannelMax() {
      return executeJoined(Connection::getChannelMax);
    }

    @Override
    default Map<String, Object> getClientProperties() {
      return executeJoined(Connection::getClientProperties);
    }

    @Override
    default String getClientProvidedName() {
      return executeJoined(Connection::getClientProvidedName);
    }

    @Override
    default ShutdownSignalException getCloseReason() {
      return executeJoined(Connection::getCloseReason);
    }

    @Override
    default ExceptionHandler getExceptionHandler() {
      return executeJoined(Connection::getExceptionHandler);
    }

    @Override
    default int getFrameMax() {
      return executeJoined(Connection::getFrameMax);
    }

    @Override
    default int getHeartbeat() {
      return executeJoined(Connection::getHeartbeat);
    }

    @Override
    default String getId() {
      return executeJoined(Connection::getId);
    }

    @Override
    default int getPort() {
      return executeJoined(Connection::getPort);
    }

    @Override
    default Map<String, Object> getServerProperties() {
      return executeJoined(Connection::getServerProperties);
    }

    @Override
    default void notifyListeners() {
      performJoined(Connection::notifyListeners);
    }

    @Override
    default boolean removeBlockedListener(BlockedListener listener) {
      return executeJoined(c -> c.removeBlockedListener(listener));
    }

    @Override
    default void removeShutdownListener(ShutdownListener listener) {
      perform(c -> c.removeShutdownListener(listener));
    }

    @Override
    default void setId(String id) {
      perform(c -> c.setId(id));
    }
  }

  interface Multiple extends ConnectionDecorator {

    @Override
    default void abort(int closeCode, String closeMessage, int timeout) {
      delegates().forEach(c -> c.abort(closeCode, closeMessage, timeout));
    }

    @Override
    default void addBlockedListener(BlockedListener listener) {
      delegates().forEach(c -> c.addBlockedListener(requireNonNull(listener)));
    }

    @Override
    default void addShutdownListener(ShutdownListener listener) {
      delegates().forEach(c -> c.addShutdownListener(requireNonNull(listener)));
    }

    @Override
    default void clearBlockedListeners() {
      delegates().forEach(Connection::clearBlockedListeners);
    }

    @Override
    default void close(int closeCode, String closeMessage, int timeout) throws IOException {
      Consumer<Connection> closer = c -> c.close(closeCode, closeMessage, timeout);
      closer.mapAndThrow(delegates());
    }

    Stream<? extends Connection> delegates();

    @Override
    default InetAddress getAddress() {
      return delegates().map(Connection::getAddress).findAny().get();
    }

    @Override
    default int getChannelMax() {
      return delegates().mapToInt(Connection::getChannelMax).min().getAsInt();
    }

    @Override
    default Map<String, Object> getClientProperties() {
      return delegates().map(Connection::getClientProperties).findAny().get();
    }

    @Override
    default String getClientProvidedName() {
      return delegates().map(Connection::getClientProvidedName).findAny().get();
    }

    @Override
    default ShutdownSignalException getCloseReason() {
      return delegates()
          .map(Connection::getCloseReason)
          .filter(Objects::nonNull)
          .findAny()
          .orElse(null);
    }

    @Override
    default ExceptionHandler getExceptionHandler() {
      return delegates().map(Connection::getExceptionHandler).findAny().get();
    }

    @Override
    default int getFrameMax() {
      return delegates().mapToInt(Connection::getFrameMax).min().getAsInt();
    }

    @Override
    default int getHeartbeat() {
      return delegates().mapToInt(Connection::getHeartbeat).findAny().getAsInt();
    }

    @Override
    default String getId() {
      return delegates().map(Connection::getId).findFirst().get();
    }

    @Override
    default int getPort() {
      return delegates().mapToInt(Connection::getPort).findAny().getAsInt();
    }

    @Override
    default Map<String, Object> getServerProperties() {
      return delegates().map(Connection::getServerProperties).findAny().get();
    }

    @Override
    default boolean isOpen() {
      return delegates().anyMatch(Connection::isOpen);
    }

    @Override
    default void notifyListeners() {
      delegates().forEach(Connection::notifyListeners);
    }

    @Override
    default boolean removeBlockedListener(BlockedListener listener) {
      return delegates().allMatch(c -> c.removeBlockedListener(listener));
    }

    @Override
    default void removeShutdownListener(ShutdownListener listener) {
      delegates().forEach(c -> c.removeShutdownListener(listener));
    }

    @Override
    default void setId(String id) {
      delegates().forEach(c -> c.setId(id));
    }
  }

  interface Single extends ConnectionDecorator {

    @Override
    default void abort(int closeCode, String closeMessage, int timeout) {
      delegate().abort(closeCode, closeMessage, timeout);
    }

    @Override
    default void addBlockedListener(BlockedListener listener) {
      delegate().addBlockedListener(listener);
    }

    @Override
    default void addShutdownListener(ShutdownListener listener) {
      delegate().addShutdownListener(listener);
    }

    @Override
    default void clearBlockedListeners() {
      delegate().clearBlockedListeners();
    }

    @Override
    default void close(int closeCode, String closeMessage, int timeout) throws IOException {
      delegate().close(closeCode, closeMessage, timeout);
    }

    Connection delegate();

    @Override
    default InetAddress getAddress() {
      return delegate().getAddress();
    }

    @Override
    default int getChannelMax() {
      return delegate().getChannelMax();
    }

    @Override
    default Map<String, Object> getClientProperties() {
      return delegate().getClientProperties();
    }

    @Override
    default String getClientProvidedName() {
      return delegate().getClientProvidedName();
    }

    @Override
    default ShutdownSignalException getCloseReason() {
      return delegate().getCloseReason();
    }

    @Override
    default ExceptionHandler getExceptionHandler() {
      return delegate().getExceptionHandler();
    }

    @Override
    default int getFrameMax() {
      return delegate().getFrameMax();
    }

    @Override
    default int getHeartbeat() {
      return delegate().getHeartbeat();
    }

    @Override
    default String getId() {
      return delegate().getId();
    }

    @Override
    default int getPort() {
      return delegate().getPort();
    }

    @Override
    default Map<String, Object> getServerProperties() {
      return delegate().getServerProperties();
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
    default boolean removeBlockedListener(BlockedListener listener) {
      return delegate().removeBlockedListener(listener);
    }

    @Override
    default void removeShutdownListener(ShutdownListener listener) {
      delegate().removeShutdownListener(listener);
    }

    @Override
    default void setId(String id) {
      delegate().setId(id);
    }
  }

  @Override
  default void abort() {
    abort(-1);
  }

  @Override
  default void abort(int timeout) {
    abort(AMQP.REPLY_SUCCESS, "OK", timeout);
  }

  @Override
  default void abort(int closeCode, String closeMessage) {
    abort(closeCode, closeMessage, -1);
  }

  @Override
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
