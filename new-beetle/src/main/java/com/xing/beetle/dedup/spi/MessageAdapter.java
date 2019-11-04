package com.xing.beetle.dedup.spi;

import static java.util.Objects.requireNonNull;
import java.util.function.Consumer;
import java.util.function.Function;

public interface MessageAdapter<M, K> {

  class Functional<M, K> implements MessageAdapter<M, K> {

    private Function<? super M, K> keyAccessor;
    private Consumer<? super M> acknowledger;
    private Consumer<? super M> delayer;

    public Functional(Function<? super M, K> keyAccessor, Consumer<? super M> acknowledger,
        Consumer<? super M> delayer) {
      this.keyAccessor = requireNonNull(keyAccessor);
      this.acknowledger = requireNonNull(acknowledger);
      this.delayer = requireNonNull(delayer);
    }

    @Override
    public void acknowledge(M message) {
      acknowledger.accept(message);
    }

    @Override
    public K keyOf(M message) {
      return keyAccessor.apply(message);
    }

    @Override
    public void requeue(M message) {
      delayer.accept(message);
    }
  }

  static <M, K> MessageAdapter<M, K> of(Function<? super M, K> keyAccessor,
      Consumer<? super M> acceptor, Consumer<? super M> rejector) {
    return new Functional<>(keyAccessor, acceptor, rejector);
  }

  void acknowledge(M message);

  K keyOf(M message);

  void requeue(M message);
}
