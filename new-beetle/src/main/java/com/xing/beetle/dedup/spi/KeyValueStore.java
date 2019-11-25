package com.xing.beetle.dedup.spi;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public interface KeyValueStore {

  class Value {

    public Value(long number) {
      this(String.valueOf(number));
    }

    public Value(String text) {
      this.text = requireNonNull(text);
    }

    private String text;

    public Value(Enum<?> value) {
      this(value.name());
    }

    public Value plus(long n) {
      return new Value(getAsNumber() + n);
    }

    public long getAsNumber() {
      return Long.parseLong(text);
    }

    public String getAsString() {
      return text;
    }

    public <E extends Enum<E>> E getAsEnum(Class<E> type) {
      return Enum.valueOf(type, text);
    }
  }

  Optional<Value> get(String key);

  default Optional<Value> getNullable(String key, Value whenNull) {
    if (key != null) {
      return get(key);
    } else {
      return Optional.ofNullable(whenNull);
    }
  }

  Value putIfAbsent(String key, Value value);

  void put(String key, Value value);

  void remove(String... keys);

  void putAll(Map<String, Value> keyValues);

  boolean putAllIfAbsent(Map<String, Value> keyValues);

  boolean exists(String key);

  List<Value> getAll(List<String> keys);

  long increase(String key);

  long decrease(String key);

  class InMemoryStore implements KeyValueStore {

    private Map<String, Value> values = new ConcurrentHashMap<>();

    @Override
    public synchronized Optional<Value> get(String key) {
      return Optional.ofNullable(values.get(key));
    }

    @Override
    public synchronized Value putIfAbsent(String key, Value value) {
      return values.computeIfAbsent(key, s -> value);
    }

    @Override
    public synchronized void put(String key, Value value) {
      values.put(key, value);
    }

    @Override
    public synchronized void remove(String... keys) {
      values.keySet().removeAll(Arrays.asList(keys));
    }

    @Override
    public synchronized void putAll(Map<String, Value> keyValues) {
      values.putAll(keyValues);
    }

    @Override
    public synchronized boolean putAllIfAbsent(Map<String, Value> keyValues) {
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
    public synchronized List<Value> getAll(List<String> keys) {
      return keys.stream().map(s -> values.get(s)).collect(Collectors.toList());
    }

    @Override
    public synchronized long increase(String key) {
      return values.compute(key, (k, v) -> v != null ? v.plus(1) : new Value(1)).getAsNumber();
    }

    @Override
    public synchronized long decrease(String key) {
      return values.compute(key, (k, v) -> v != null ? v.plus(-1) : new Value(-1)).getAsNumber();
    }
  }
}
