package com.xing.beetle.util;

import java.util.concurrent.*;

public class DelayedExecutor implements Executor {
  private static final Executor ASYNC_POOL = ForkJoinPool.commonPool();

  final long delay;
  final TimeUnit unit;
  final Executor executor;

  DelayedExecutor(long delay, TimeUnit unit, Executor executor) {
    this.delay = delay;
    this.unit = unit;
    this.executor = executor;
  }

  public void execute(Runnable r) {
    Delayer.delay(new TaskSubmitter(executor, r), delay, unit);
  }

  /** Singleton delay scheduler, used only for starting and cancelling tasks. */
  static final class Delayer {
    static ScheduledFuture<?> delay(Runnable command, long delay, TimeUnit unit) {
      return delayer.schedule(command, delay, unit);
    }

    static final class DaemonThreadFactory implements ThreadFactory {
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("BeetleDelayScheduler");
        return t;
      }
    }

    static final ScheduledThreadPoolExecutor delayer;

    static {
      (delayer = new ScheduledThreadPoolExecutor(1, new Delayer.DaemonThreadFactory()))
          .setRemoveOnCancelPolicy(true);
    }
  }

  /** Action to submit user task */
  static final class TaskSubmitter implements Runnable {
    final Executor executor;
    final Runnable action;

    TaskSubmitter(Executor executor, Runnable action) {
      this.executor = executor;
      this.action = action;
    }

    public void run() {
      executor.execute(action);
    }
  }

  public static Executor delayedExecutor(long delay, TimeUnit unit, Executor executor) {
    if (unit == null || executor == null) throw new NullPointerException();
    return new DelayedExecutor(delay, unit, executor);
  }
  /**
   * Returns a new Executor that submits a task to the default executor after the given delay (or no
   * delay if non-positive). Each delay commences upon invocation of the returned executor's {@code
   * execute} method.
   *
   * @param delay how long to delay, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code delay} parameter
   * @return the new delayed executor
   * @since 9
   */
  public static Executor delayedExecutor(long delay, TimeUnit unit) {
    if (unit == null) throw new NullPointerException();
    return new DelayedExecutor(delay, unit, ASYNC_POOL);
  }
}
