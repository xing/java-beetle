package com.xing.beetle.util;

import static java.util.Objects.requireNonNull;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Agent<T> implements Async<T> {

  private static class Task<T, R> {

    private final Action<? super T, ? extends R> action;
    private final CompletableFuture<R> future;

    Task(Action<? super T, ? extends R> action) {
      this.action = requireNonNull(action);
      this.future = new CompletableFuture<>();
    }

    void execute(T instance) {
      if (future.isDone()) {
        return;
      }
      try {
        R result = action.doExecute(instance);
        future.complete(result);
      } catch (Throwable e) {
        future.completeExceptionally(e);
      }
    }

    CompletionStage<R> stage() {
      return future;
    }
  }

  private static final int MAX_TASK_BATCH = 100;

  private final T instance;
  private final Executor executor;
  private final Queue<Task<T, ?>> tasks;
  private final AtomicBoolean scheduled;

  public Agent(T instance, Executor executor) {
    this.instance = requireNonNull(instance);
    this.executor = requireNonNull(executor);
    this.tasks = new ConcurrentLinkedQueue<>();
    this.scheduled = new AtomicBoolean(false);
  }

  @Override
  public <R> CompletionStage<R> execute(Action<? super T, ? extends R> action) {
    Task<T, R> task = new Task<>(action);
    if (tasks.offer(task) && scheduled.compareAndSet(false, true)) {
      executor.execute(this::executeTasks);
    }
    return task.stage();
  }

  private void executeTasks() {
    int executed = 0;
    Task<T, ?> task = null;
    while (executed++ < MAX_TASK_BATCH && (task = tasks.poll()) != null) {
      task.execute(instance);
    }
    if (tasks.isEmpty()) {
      scheduled.set(false);
    } else {
      executor.execute(this::executeTasks);
    }
  }
}
