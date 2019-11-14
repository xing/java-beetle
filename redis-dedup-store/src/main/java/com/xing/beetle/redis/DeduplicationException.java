package com.xing.beetle.redis;

public class DeduplicationException extends RuntimeException {

  public DeduplicationException(String message, Throwable e) {
    super(message, e);
  }
}
