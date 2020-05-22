package com.xing.beetle.redis;

class DeduplicationException extends RuntimeException {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  DeduplicationException(String message, Throwable e) {
    super(message, e);
  }
}
