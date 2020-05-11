package com.xing.beetle.util;

import static java.util.Objects.requireNonNull;

import com.xing.beetle.util.ExceptionSupport.Consumer;
import com.xing.beetle.util.ExceptionSupport.Function;
import com.xing.beetle.util.ExceptionSupport.Runnable;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

public class OrderedPromise<T> {

  public static <T> OrderedPromise<T> of(CompletionStage<T> delegate) {
    return new OrderedPromise<>(delegate, CompletableFuture.completedStage(null));
  }

  private final CompletionStage<T> delegate;
  private final CompletionStage<?> dependent;

  private OrderedPromise(CompletionStage<T> delegate, CompletionStage<?> dependent) {
    this.delegate = requireNonNull(delegate);
    this.dependent = requireNonNull(dependent);
  }

  private <X> OrderedPromise<X> chain(CompletionStage<X> stage) {
    return new OrderedPromise<>(stage, delegate);
  }

  public Optional<T> getNow() throws CancellationException {
    try {
      return Optional.ofNullable(delegate.toCompletableFuture().getNow(null));
    } catch (CompletionException e) {
      return ExceptionSupport.sneakyThrow(e.getCause());
    }
  }

  public T join() {
    return delegate.toCompletableFuture().join();
  }

  public OrderedPromise<Void> thenAccept(Consumer<? super T> action) {
    return chain(delegate.thenAcceptBoth(dependent, (t, x) -> action.accept(t)));
  }

  public <U> OrderedPromise<U> thenApply(Function<? super T, ? extends U> fn) {
    return chain(delegate.thenCombine(dependent, (t, x) -> fn.apply(t)));
  }

  public OrderedPromise<Void> thenRun(Runnable action) {
    return chain(delegate.runAfterBoth(dependent, action));
  }

  public CompletionStage<T> toStage() {
    return delegate;
  }
}
