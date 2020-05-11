package com.xing.beetle.util;

import com.xing.beetle.util.ExceptionSupport.Supplier;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.*;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.ThreadLocalRandom.current;

public class RetryExecutor {

  @FunctionalInterface
  public interface Backoff {

    Backoff DEFAULT = exponential(1, TimeUnit.SECONDS).withUniformJitter(100).withMaxAttempts(8);

    static Backoff exponential(long delay, TimeUnit unit) {
      return (att, err) -> (long) Math.pow(2, att) * unit.toMillis(delay);
    }

    static Backoff fixed(long delay, TimeUnit unit) {
      return (att, err) -> att == 0 ? 0 : unit.toMillis(delay);
    }

    static Backoff linear(long delay, TimeUnit unit) {
      return (att, err) -> att * unit.toMillis(delay);
    }

    long delayInMillis(int attempt, Throwable error);

    default Backoff withMaxAttempts(int max) {
      return (att, err) -> att < max ? delayInMillis(att, err) : -1;
    }

    default Backoff withMaxDelay(long delay, TimeUnit unit) {
      return (att, err) -> Math.min(delayInMillis(att, err), unit.toMillis(delay));
    }

    default Backoff withMinDelay(long delay, TimeUnit unit) {
      return (att, err) -> Math.max(delayInMillis(att, err), unit.toMillis(delay));
    }

    default Backoff withProportionalJitter(double factor) {
      return (att, err) ->
          (long) (delayInMillis(att, err) * current().nextDouble(1d - factor, 1d + factor));
    }

    default Backoff withRetryOn(Class<? extends Throwable> errorType) {
      return withRetryOn(errorType::isInstance);
    }

    default <E extends Throwable> Backoff withRetryOn(Class<E> errorType, Predicate<E> when) {
      return withRetryOn(ex -> errorType.isInstance(ex) && when.test(errorType.cast(ex)));
    }

    default Backoff withRetryOn(Predicate<? super Throwable> when) {
      return (att, err) -> err == null || when.test(err) ? delayInMillis(att, err) : -1;
    }

    default Backoff withUniformJitter(long millis) {
      return (att, err) ->
          Math.max(0, delayInMillis(att, err) + current().nextLong(-millis, millis));
    }
  }

  private class Retrying<T> implements Runnable {

    private final Supplier<? extends T> supplier;
    private final Logger logger;
    private final CompletableFuture<T> future;
    private int attempt;

    Retrying(Supplier<? extends T> supplier) {
      this.supplier = requireNonNull(supplier);
      this.logger = System.getLogger(supplier.getClass().getName());
      this.future = new CompletableFuture<>();
      this.attempt = 0;
      future.whenComplete(this::logCompletion);
    }

    private void logCompletion(T success, Throwable error) {
      if (error != null) {
        logger.log(Level.ERROR, "Failed to supply. Stop retrying.", error);
      } else if (logger.isLoggable(logLevel)) {
        logger.log(logLevel, "Finished to supply " + success);
      }
    }

    @Override
    public void run() {
      try {
        T result = supplier.getChecked();
        future.complete(result);
      } catch (Throwable error) {
        if (logger.isLoggable(logLevel)) {
          logger.log(logLevel, String.format("%d. attempt failed", attempt), error);
        }
        schedule(error);
      }
    }

    CompletionStage<T> schedule(Throwable error) {
      long delayInMillis = backoff.delayInMillis(attempt++, error);
      if (delayInMillis > 0) {
        scheduler.delayed(delayInMillis, TimeUnit.MILLISECONDS, executor).execute(this);
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

  @FunctionalInterface
  public interface Scheduler {

    Scheduler DEFAULT = CompletableFuture::delayedExecutor;
    Scheduler IMMEDIATELY = (delay, unit, executor) -> executor;
    Scheduler SYNCHRONOUS = (delay, unit, executor) -> Runnable::run;

    Executor delayed(long delay, TimeUnit unit, Executor executor);
  }

  public static RetryExecutor ASYNC_EXPONENTIAL =
      new RetryExecutor(ForkJoinPool.commonPool(), Scheduler.DEFAULT, Backoff.DEFAULT, Level.DEBUG);
  public static RetryExecutor ASYNC_IMMEDIATELY =
      new RetryExecutor(
          ForkJoinPool.commonPool(), Scheduler.IMMEDIATELY, Backoff.DEFAULT, Level.DEBUG);
  public static RetryExecutor SYNCHRONOUS =
      new RetryExecutor(Runnable::run, Scheduler.SYNCHRONOUS, Backoff.DEFAULT, Level.DEBUG);
  public static RetryExecutor DEFAULT =
      new RetryExecutor(Runnable::run, Scheduler.DEFAULT, Backoff.DEFAULT, Level.DEBUG);

  private final Executor executor;
  private final Scheduler scheduler;
  private final Backoff backoff;
  private final Level logLevel;

  public RetryExecutor(Executor executor, Scheduler scheduler, Backoff backoff, Level level) {
    this.executor = requireNonNull(executor);
    this.scheduler = requireNonNull(scheduler);
    this.backoff = requireNonNull(backoff);
    this.logLevel = requireNonNull(level);
  }

  public <T> CompletionStage<T> supply(Supplier<? extends T> supplier) {
    return new Retrying<T>(supplier).schedule(null);
  }

  public RetryExecutor withBackoff(Backoff backoff) {
    return new RetryExecutor(executor, scheduler, backoff, logLevel);
  }

  public RetryExecutor withExecutor(Executor executor) {
    return new RetryExecutor(executor, scheduler, backoff, logLevel);
  }

  public RetryExecutor withLogLevel(Level logLevel) {
    return new RetryExecutor(executor, scheduler, backoff, logLevel);
  }

  public RetryExecutor withScheduler(Scheduler scheduler) {
    return new RetryExecutor(executor, scheduler, backoff, logLevel);
  }
}
