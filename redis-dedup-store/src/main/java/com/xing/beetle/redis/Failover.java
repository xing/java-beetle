package com.xing.beetle.redis;

import com.xing.beetle.util.ExceptionSupport;
import com.xing.beetle.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.xing.beetle.util.RetryExecutor.Backoff.linear;

/**
 * Provides execution of any method with failover logic which uses retries and timeout. Retries are
 * linear with 1 sec delay.
 */
class Failover {

  private static Logger logger = LoggerFactory.getLogger(Failover.class);
  private final int timeout;
  private final int maxRetries;
  private final RetryExecutor retryExecutor;

  Failover(int timeout, int maxRetries) {
    this.timeout = timeout;
    this.maxRetries = maxRetries;
    RetryExecutor.Backoff backoff =
        linear(1, TimeUnit.SECONDS).withUniformJitter(100).withMaxAttempts(this.maxRetries);
    retryExecutor = RetryExecutor.SYNCHRONOUS.withBackoff(backoff);
  }

  public <T> T execute(ExceptionSupport.Supplier<? extends T> supplier) {
    try {
      return retryExecutor.supply(supplier).toCompletableFuture().get(timeout, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      logger.warn("Redis request timed out");
      throw new DeduplicationException("Request timed out", e);
    } catch (Exception e) {
      throw new DeduplicationException("Request failed", e);
    }
  }
}
