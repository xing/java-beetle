package com.xing.beetle.dedup.spi;

public interface MessageAdapter<M> {

  void acknowledge(M message);

  String keyOf(M message);

  void requeue(M message);

  long expiresAt(M message);
}
