package com.xing.beetle.redis;

import com.xing.beetle.util.ExceptionSupport;
import com.xing.beetle.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static com.xing.beetle.util.RetryExecutor.Backoff.fixed;

/**
 * Provides execution of any method with failover logic which uses retries and timeout. Retries are
 * backed off linearly starting with 1 sec delay.
 */
class Failover {

  private static Logger logger = LoggerFactory.getLogger(Failover.class);
  private final int timeout;
  private final RetryExecutor retryExecutor;
  private ReentrantLock lock = new ReentrantLock();

  Failover(int timeoutInSeconds, int retryIntervalInSeconds) {
    this.timeout = timeoutInSeconds;
    RetryExecutor.Backoff backoff = fixed(retryIntervalInSeconds, TimeUnit.SECONDS);
    retryExecutor = new RetryExecutor.Builder().backOff(backoff).build();
  }

  <T> T execute(ExceptionSupport.Supplier<? extends T> supplier) {
    lock.lock();
    try {
      return retryExecutor.supply(supplier).toCompletableFuture().get(timeout, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      logger.warn("Redis request timed out");
      throw new DeduplicationException("Deduplication store request timed out", e);
    } catch (Exception e) {
      throw new DeduplicationException("Deduplication store request failed", e);
    } finally {
      lock.unlock();
    }
  }
}
