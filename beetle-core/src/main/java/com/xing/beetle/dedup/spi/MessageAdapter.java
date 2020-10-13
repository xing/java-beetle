package com.xing.beetle.dedup.spi;

public interface MessageAdapter<M> {

  void drop(M message);

  String messageId(M message);

  void requeue(M message);

  long expiresAt(M message);

  boolean isRedundant(M message);
}
