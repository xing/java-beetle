package com.xing.beetle.dedup.spi;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface KeyValueStore {

  void delete(String key);

  class Value {

    Value(long number) {
      this(String.valueOf(number));
    }

    public Value(String text) {
      this.text = requireNonNull(text);
    }

    private String text;

    long getAsNumber() {
      return Long.parseLong(text);
    }

    public String getAsString() {
      return text;
    }
  }

  Optional<Value> get(String key);

  Value putIfAbsent(String key, Value value);

  boolean putIfAbsentTtl(String key, Value value, int secondsToExpire);

  void put(String key, Value value);

  void remove(String... keys);

  long increase(String key);
}
