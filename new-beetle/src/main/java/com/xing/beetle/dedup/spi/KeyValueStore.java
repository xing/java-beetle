package com.xing.beetle.dedup.spi;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public interface KeyValueStore<V> {

  Optional<V> get(String key);

  default Optional<V> getNullable(String key, V whenNull) {
    if (key != null) {
      return get(key);
    } else {
      return Optional.ofNullable(whenNull);
    }
  }

  boolean putIfAbsent(String key, V value);

  void put(String key, V value);

  void remove(String key);

  default <X> KeyValueStore<X> suffixed(
      String suffix, Function<? super V, ? extends X> map1, Function<? super X, ? extends V> map2) {
    return new KeyValueStore<X>() {
      @Override
      public Optional<X> get(String key) {
        return KeyValueStore.this.get(key + suffix).map(map1);
      }

      @Override
      public boolean putIfAbsent(String key, X value) {
        return KeyValueStore.this.putIfAbsent(key + suffix, map2.apply(value));
      }

      @Override
      public void put(String key, X value) {
        KeyValueStore.this.put(key + suffix, map2.apply(value));
      }

      @Override
      public void remove(String key) {
        KeyValueStore.this.remove(key + suffix);
      }
    };
  }

  default KeyValueStore<V> suffixed(String suffix) {
    return suffixed(suffix, Function.identity(), Function.identity());
  }

  class InMemoryStore implements KeyValueStore<String> {

    private Map<String, String> values = new ConcurrentHashMap<>();

    @Override
    public Optional<String> get(String key) {
      return Optional.ofNullable(values.get(key));
    }

    @Override
    public boolean putIfAbsent(String key, String value) {
      return values.putIfAbsent(key, value) == null;
    }

    @Override
    public void put(String key, String value) {
      values.put(key, value);
    }

    @Override
    public void remove(String key) {
      values.remove(key);
    }
  }
}
