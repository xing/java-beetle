package com.xing.beetle.dedup;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.Executor;

import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.KeyValueStore;
import com.xing.beetle.dedup.spi.MessageAdapter;

public class MessageHandlingState {

    public interface Outcome<M> {

        void apply(MessageAdapter<M> adapter, KeyValueStore<String> store, Executor executor);
    }

    public enum Status {
        INCOMPLETE {
            @Override
            public <M> Outcome<M> handle(M message, MessageListener<M> listener) {
                return (adapter, store, executor) -> {
                    String key = adapter.keyOf(message);
                    KeyValueStore<Long> mutex = store.suffixed("mutex", Long::valueOf, v -> v.toString());
                    if (mutex.putIfAbsent(key, System.currentTimeMillis())) {
                        try {
                            listener.onMessage(message);
                            store.suffixed("status", Status::valueOf, Status::toString).put(key, Status.COMPLETE);
                        } catch (Throwable throwable) {
                            throwable.printStackTrace();
                        } finally {
                            mutex.remove(key);
                        }
                    } else {
                        adapter.acknowledge(message);
                    }
                };
            }
        }, COMPLETE {
            @Override
            public <M> Outcome<M> handle(M message, MessageListener<M> listener) {
                return (adapter, store, executor) -> {
                    adapter.acknowledge(message);
                };
            }
        };

        public abstract <M> Outcome<M> handle(M message, MessageListener<M> listener);
    }
}
