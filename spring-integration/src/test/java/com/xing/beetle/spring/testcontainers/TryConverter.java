package com.xing.beetle.spring.testcontainers;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@FunctionalInterface
public interface TryConverter<T> {

  static <T> TryConverter<T> of(Class<T> type) {
    Objects.requireNonNull(type);
    return any -> Optional.ofNullable(any).filter(type::isInstance).map(type::cast);
  }

  default TryConverter<T> filter(Predicate<? super T> predicate) {
    Objects.requireNonNull(predicate);
    return any -> tryConvert(any).filter(predicate);
  }

  default <R> TryConverter<R> flatMap(Class<R> type) {
    return flatMap(of(type)::tryConvert);
  }

  default <R> TryConverter<R> flatMap(Function<? super T, Optional<? extends R>> mapper) {
    Objects.requireNonNull(mapper);
    return any -> tryConvert(any).flatMap(mapper);
  }

  default <R> TryConverter<R> map(Function<? super T, ? extends R> mapper) {
    Objects.requireNonNull(mapper);
    return any -> tryConvert(any).map(mapper);
  }

  default TryConverter<T> or(TryConverter<? extends T> other) {
    Objects.requireNonNull(other);
    return any -> this.tryConvert(any).or(() -> other.tryConvert(any));
  }

  default <R> TryConverter<R> supplyIfMatches(
		  Predicate<? super T> predicate, Supplier<? extends R> supplier) {
    Objects.requireNonNull(predicate);
    Objects.requireNonNull(supplier);
    return map(t -> predicate.test(t) ? supplier.get() : null);
  }

  Optional<T> tryConvert(Object any);
}
