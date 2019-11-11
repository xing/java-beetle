package com.xing.beetle.dedup.spi;

import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public interface MessageAdapter<M> {

    class Functional<M> implements MessageAdapter<M> {

        private Function<? super M, String> keyAccessor;
        private Consumer<? super M> acknowledger;
        private Consumer<? super M> delayer;

        Functional(Function<? super M, String> keyAccessor, Consumer<? super M> acknowledger,
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
        public String keyOf(M message) {
            return keyAccessor.apply(message);
        }

        @Override
        public void requeue(M message) {
            delayer.accept(message);
        }
    }

    static <M> MessageAdapter<M> of(Function<? super M, String> keyAccessor,
                                    Consumer<? super M> acceptor, Consumer<? super M> rejector) {
        return new Functional<>(keyAccessor, acceptor, rejector);
    }

    void acknowledge(M message);

    String keyOf(M message);

    void requeue(M message);
}
