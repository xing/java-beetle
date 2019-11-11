package com.xing.beetle.dedup.spi;

import com.xing.beetle.dedup.MessageHandlingState;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public interface DedupStore<K> {

    class InMemoryStore<K> implements DedupStore<K> {

        private Map<K, MessageHandlingState<K>> data = new ConcurrentHashMap<>();

        @Override
        public void deleteMutex(K key) {

        }

        @Override
        public Optional<MessageHandlingState<K>> tryFind(K key) {
            return Optional.ofNullable(data.get(key));
        }

        @Override
        public boolean setMutex(K key) {
            return false;
        }

        @Override
        public boolean tryInsert(MessageHandlingState<K> state) {
            return false;
        }

        @Override
        public boolean update(MessageHandlingState<K> state) {
            return false;
        }
    }

    void deleteMutex(K key);

    Optional<MessageHandlingState<K>> tryFind(K key);

    default MessageHandlingState<K> find(K key) {
        return tryFind(key).orElse(MessageHandlingState.initialTry(key));
    }

    boolean setMutex(K key);

    boolean tryInsert(MessageHandlingState<K> state);

    boolean update(MessageHandlingState<K> state);
}
