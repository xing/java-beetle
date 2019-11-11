package com.xing.beetle.dedup.spi;

import com.xing.beetle.dedup.MessageHandlingState;

public interface DedupStore<K> {

  void deleteMutex(K key);

  MessageHandlingState<K> retrieve(K key);

  boolean setMutex(K key);

  boolean tryInsert(MessageHandlingState<K> state);

  boolean update(MessageHandlingState<K> state);
}
