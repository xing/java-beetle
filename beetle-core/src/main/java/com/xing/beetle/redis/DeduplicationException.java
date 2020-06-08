package com.xing.beetle.redis;

class DeduplicationException extends RuntimeException {

  /** Exception thrown when an error occurs during deduplication. */
  private static final long serialVersionUID = 1L;

  DeduplicationException(String message, Throwable e) {
    super(message, e);
  }
}
