package com.xing.beetle.util;

import com.xing.beetle.util.ExceptionSupport.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static java.util.Objects.requireNonNull;

public class RetryExecutor {

  static final ScheduledExecutorService scheduledExecutorService =
      Executors.newScheduledThreadPool(2);

  @FunctionalInterface
  public interface Backoff {

    Backoff DEFAULT = linear(1, TimeUnit.SECONDS).withMaxAttempts(8);

    static Backoff linear(long delay, TimeUnit unit) {
      return (att, err) -> att * unit.toMillis(delay);
    }

    static Backoff fixed(long delay, TimeUnit unit) {
      return (att, err) -> att == 0 ? 0 : unit.toMillis(delay);
    }

    long delayInMillis(int attempt, Throwable error);

    default Backoff withMaxAttempts(int max) {
      return (att, err) -> att < max ? delayInMillis(att, err) : -1;
    }
  }

  private class Retrying<T> implements Runnable {

    private final Supplier<? extends T> supplier;
    private final Logger logger;
    private final CompletableFuture<T> future;
    private int attempt;

    Retrying(Supplier<? extends T> supplier) {
      this.supplier = requireNonNull(supplier);
      this.logger = LoggerFactory.getLogger(supplier.getClass());
      this.future = new CompletableFuture<>();
      this.attempt = 0;
      future.whenComplete(this::logCompletion);
    }

    private void logCompletion(T success, Throwable error) {
      if (error != null) {
        logger.error("Failed to supply. Stop retrying.", error);
      } else if (logger.isDebugEnabled()) {
        logger.debug("Finished to supply " + success);
      }
    }

    @Override
    public void run() {
      try {
        T result = supplier.getChecked();
        future.complete(result);
      } catch (Throwable error) {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("%d. attempt failed", attempt), error);
        }
        schedule(error);
      }
    }

    CompletionStage<T> schedule(Throwable error) {
      long delayInMillis = backoff.delayInMillis(attempt++, error);
      if (delayInMillis > 0) {
        scheduledExecutorService.schedule(this, delayInMillis, TimeUnit.MILLISECONDS);
      } else if (delayInMillis == 0) {
        executor.execute(this);
      } else if (error != null) {
        future.completeExceptionally(error);
      } else {
        future.cancel(false);
      }
      return future;
    }
  }

  private Executor executor;
  private Backoff backoff;

  private RetryExecutor(Executor executor, Backoff backoff) {
    this.executor = requireNonNull(executor);
    this.backoff = requireNonNull(backoff);
  }

  public <T> CompletionStage<T> supply(Supplier<? extends T> supplier) {
    return new Retrying<T>(supplier).schedule(null);
  }

  public static class Builder {
    protected Executor executor = ForkJoinPool.commonPool();
    protected Backoff backoff = Backoff.DEFAULT;

    public Builder executor(Executor executor) {
      this.executor = executor;
      return this;
    }

    public Builder backOff(Backoff backoff) {
      this.backoff = backoff;
      return this;
    }

    public RetryExecutor build() {
      return new RetryExecutor(executor, backoff);
    }
  }
}
