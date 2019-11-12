package com.xing.beetle.util;

import static java.util.Objects.requireNonNull;

import com.xing.beetle.util.ExceptionSupport.Consumer;
import com.xing.beetle.util.ExceptionSupport.Function;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

public class OrderedPromise<T> {

  private final CompletableFuture<T> parent;
  private final List<CompletableFuture<?>> children;

  public OrderedPromise(CompletionStage<T> parent) {
    this.parent = requireNonNull(parent).toCompletableFuture();
    this.children = Collections.synchronizedList(new ArrayList<>());
  }

  private <X> OrderedPromise<X> chain(CompletableFuture<X> future) {
    children.add(future);
    return new OrderedPromise<>(future);
  }

  private CompletionStage<Void> childs() {
    return CompletableFuture.allOf(children.toArray(new CompletableFuture[children.size()]));
  }

  public Optional<T> get() throws CancellationException {
    try {
      return Optional.ofNullable(parent.getNow(null));
    } catch (CompletionException e) {
      return ExceptionSupport.sneakyThrow(e.getCause());
    }
  }

  public OrderedPromise<Void> thenAccept(Consumer<? super T> action) {
    return chain(parent.thenAcceptBoth(childs(), (t, v) -> action.accept(t)));
  }

  public <X> OrderedPromise<X> thenApply(Function<? super T, ? extends X> fn) {
    return chain(parent.thenCombine(childs(), (t, v) -> fn.apply(t)));
  }

  public OrderedPromise<Void> thenRun(Runnable action) {
    return chain(parent.runAfterBoth(childs(), action));
  }

  public CompletionStage<T> toStage() {
    return parent;
  }
}
