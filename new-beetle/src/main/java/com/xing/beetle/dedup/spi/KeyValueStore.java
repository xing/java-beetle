package com.xing.beetle.dedup.spi;

public interface KeyValueStore<K, V> {

  V retrieve(K key);

  boolean tryInsert(K key, V value);

  boolean update(K key, V value);
}
