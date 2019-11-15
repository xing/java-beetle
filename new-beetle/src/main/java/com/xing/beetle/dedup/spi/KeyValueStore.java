package com.xing.beetle.dedup.spi;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  void remove(String... keys);

  void putAll(Map<String, V> keyValues);

  boolean putAllIfAbsent(Map<String, V> keyValues);

  boolean exists(String key);

  List<V> getAll(List<String> keys);

  V increase(String key);

  V decrease(String key);

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
      public void remove(String... keys) {
        List<String> mappedKeys =
            Stream.of(keys).map(key -> key + suffix).collect(Collectors.toList());
        KeyValueStore.this.remove(mappedKeys.stream().toArray(String[]::new));
      }

      @Override
      public void putAll(Map<String, X> keyValues) {
        Map<String, V> map = new HashMap<>();
        keyValues.forEach(
            (key, value) -> {
              map.put(key + suffix, map2.apply(value));
            });
        KeyValueStore.this.putAll(map);
      }

      @Override
      public boolean putAllIfAbsent(Map<String, X> keyValues) {
        Map<String, V> map = new HashMap<>();
        keyValues.forEach(
            (key, value) -> {
              map.put(key + suffix, map2.apply(value));
            });
        return KeyValueStore.this.putAllIfAbsent(map);
      }

      @Override
      public boolean exists(String key) {
        return KeyValueStore.this.exists(key + suffix);
      }

      @Override
      public List<X> getAll(List<String> keys) {
        List<String> mappedKeys = keys.stream().map(s -> s + suffix).collect(Collectors.toList());
        return KeyValueStore.this.getAll(mappedKeys).stream()
            .map(map1)
            .collect(Collectors.toList());
      }

      @Override
      public X increase(String key) {
        return Optional.of(KeyValueStore.this.increase(key + suffix)).map(map1).get();
      }

      @Override
      public X decrease(String key) {
        return Optional.of(KeyValueStore.this.decrease(key + suffix)).map(map1).get();
      }
    };
  }

  default KeyValueStore<V> suffixed(String suffix) {
    return suffixed(suffix, Function.identity(), Function.identity());
  }

  class InMemoryStore implements KeyValueStore<String> {

    private Map<String, String> values = new ConcurrentHashMap<>();

    @Override
    public synchronized Optional<String> get(String key) {
      return Optional.ofNullable(values.get(key));
    }

    @Override
    public synchronized boolean putIfAbsent(String key, String value) {
      return values.putIfAbsent(key, value) == null;
    }

    @Override
    public synchronized void put(String key, String value) {
      values.put(key, value);
    }

    @Override
    public synchronized void remove(String... keys) {
      values.keySet().removeAll(Arrays.asList(keys));
    }

    @Override
    public synchronized void putAll(Map<String, String> keyValues) {
      values.putAll(keyValues);
    }

    @Override
    public synchronized boolean putAllIfAbsent(Map<String, String> keyValues) {
      if (values.keySet().stream().noneMatch(keyValues::containsKey)) {
        values.putAll(keyValues);
        return true;
      }
      return false;
    }

    @Override
    public synchronized boolean exists(String key) {
      return values.containsKey(key);
    }

    @Override
    public synchronized List<String> getAll(List<String> keys) {
      return keys.stream().map(s -> values.get(s)).collect(Collectors.toList());
    }

    @Override
    public synchronized String increase(String key) {
      long incremented = Long.parseLong(values.get(key)) + 1;
      values.put(key, String.valueOf(incremented));
      return String.valueOf(incremented);
    }

    @Override
    public synchronized String decrease(String key) {
      long decremented = Long.parseLong(values.get(key)) - 1;
      values.put(key, String.valueOf(decremented));
      return String.valueOf(decremented);
    }
  }
}
