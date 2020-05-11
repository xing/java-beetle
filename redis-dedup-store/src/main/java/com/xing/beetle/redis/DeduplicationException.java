package com.xing.beetle.redis;

class DeduplicationException extends RuntimeException {

  DeduplicationException(String message, Throwable e) {
    super(message, e);
  }
}
