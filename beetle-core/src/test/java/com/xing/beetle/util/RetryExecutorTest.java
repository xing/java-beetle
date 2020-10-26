package com.xing.beetle.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class RetryExecutorTest {

  int attempt = 1;

  @Test
  public void testDefaultBackOff() throws InterruptedException, ExecutionException {
    RetryExecutor.Builder builder = new RetryExecutor.Builder();
    RetryExecutor retryExecutor =
        builder.executor(ForkJoinPool.commonPool()).backOff(RetryExecutor.Backoff.DEFAULT).build();
    attempt = 1;
    final List<Instant> timeOfRetries = new ArrayList<>();
    final int maxAttempt = 3;
    retryExecutor.supply(() -> setTime(timeOfRetries, maxAttempt)).toCompletableFuture().get();
    assertEquals(3, timeOfRetries.size());
    assertFalse(timeOfRetries.get(0).plusMillis(1000).isAfter(timeOfRetries.get(1)));
    assertFalse(timeOfRetries.get(1).plusMillis(2000).isAfter(timeOfRetries.get(2)));
  }

  @Test
  public void testFixedBackOff() throws InterruptedException, ExecutionException {
    RetryExecutor.Builder builder = new RetryExecutor.Builder();
    int delay = 500;
    RetryExecutor retryExecutor =
        builder
            .executor(ForkJoinPool.commonPool())
            .backOff(RetryExecutor.Backoff.fixed(delay, TimeUnit.MILLISECONDS))
            .build();
    attempt = 1;
    final List<Instant> timeOfRetries = new ArrayList<>();
    final int maxAttempt = 3;
    retryExecutor.supply(() -> setTime(timeOfRetries, maxAttempt)).toCompletableFuture().get();
    assertEquals(3, timeOfRetries.size());
    assertFalse(timeOfRetries.get(0).plusMillis(delay).isAfter(timeOfRetries.get(1)));
    assertFalse(timeOfRetries.get(1).plusMillis(delay).isAfter(timeOfRetries.get(2)));
  }

  private boolean setTime(List<Instant> timeOfRetries, int maxAttempt) {
    timeOfRetries.add(Instant.now());
    if (attempt < maxAttempt) {
      attempt++;
      throw new RuntimeException();
    }
    return true;
  }
}
