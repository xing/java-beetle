package com.xing.beetle.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

@FunctionalInterface
public interface Async<T> {

  @FunctionalInterface
  interface Action<T, R> {

    R doExecute(T instance) throws Exception;

    default Result<R> doResulting(T instance) {
      try {
        R value = doExecute(instance);
        return Result.succeed(value);
      } catch (Exception e) {
        return Result.failed(e);
      }
    }

    default R doSneaky(T instance) {
      try {
        return doExecute(instance);
      } catch (Exception e) {
        return Result.Function.sneakyThrow(e);
      }
    }

    default Stream<Exception> mapAndCatch(Stream<? extends T> channels) {
      return channels.map(this::doResulting).filter(Result::isFailed).map(Result::reason);
    }
  }

  @FunctionalInterface
  interface Task<T> extends Action<T, Void> {

    @Override
    default Void doExecute(T instance) throws Exception {
      doPerform(instance);
      return null;
    }

    void doPerform(T instance) throws Exception;
  }

  <R> CompletionStage<R> execute(Action<? super T, ? extends R> action);

  default <R> R executeJoined(Action<? super T, ? extends R> action) {
    try {
      return execute(action).toCompletableFuture().join();
    } catch (CompletionException e) {
      return ExceptionSupport.sneakyThrow(e.getCause());
    }
  }

  default CompletionStage<Void> perform(Task<? super T> task) {
    return execute(task);
  }

  default void performJoined(Task<? super T> task) {
    executeJoined(task);
  }
}
