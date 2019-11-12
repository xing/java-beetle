package com.xing.beetle.util;

import static java.util.Objects.requireNonNull;

import java.util.Optional;

@FunctionalInterface
public interface Result<T> {

  class Failure<T> implements Result<T> {

    private final Exception exception;

    public Failure(Exception exception) {
      this.exception = requireNonNull(exception);
    }

    @Override
    public <R> R apply(
        Function<? super T, ? extends R> success, Function<Exception, ? extends R> failure) {
      return failure.apply(exception);
    }
  }

  @FunctionalInterface
  interface Function<T, R> extends java.util.function.Function<T, R> {

    static <T> Function<T, T> identity() {
      return t -> t;
    }

    @SuppressWarnings("unchecked")
    static <E extends Throwable, R> R sneakyThrow(Throwable exception) throws E {
      throw (E) exception;
    }

    default <X> Function<T, X> andThen(Function<? super R, ? extends X> after) {
      return t -> after.apply(this.apply(t));
    }

    @Override
    default R apply(T value) {
      try {
        return applyChecked(value);
      } catch (Exception e) {
        return sneakyThrow(e);
      }
    }

    R applyChecked(T value) throws Exception;

    default Result<R> applyFlatten(T value) {
      try {
        return succeed(applyChecked(value));
      } catch (Exception e) {
        return failed(e);
      }
    }
  }

  class Success<T> implements Result<T> {

    private final T value;

    public Success(T value) {
      this.value = value;
    }

    @Override
    public <R> R apply(
        Function<? super T, ? extends R> success, Function<Exception, ? extends R> failure) {
      return success.apply(value);
    }
  }

  static <T> Result<T> failed(Exception exception) {
    return new Failure<>(exception);
  }

  static <T> Result<T> of(T value, Exception exception) {
    return exception != null ? failed(exception) : succeed(value);
  }

  static <T> Result<T> succeed(T value) {
    return new Success<>(value);
  }

  default <R> R apply(Function<? super T, ? extends R> success) {
    return apply(success, Function::<RuntimeException, R>sneakyThrow);
  }

  <R> R apply(Function<? super T, ? extends R> success, Function<Exception, ? extends R> failure);

  default <R> Result<R> flatMap(Function<? super T, Result<R>> success) {
    return flatMap(success, Result::failed);
  }

  default <R> Result<R> flatMap(
      Function<? super T, Result<R>> success, Function<Exception, Result<R>> failure) {
    return apply(success, failure);
  }

  default T get() {
    return apply(Function.identity(), Function::<RuntimeException, T>sneakyThrow);
  }

  default boolean isFailed() {
    return !isSucceed();
  }

  default boolean isSucceed() {
    return apply(r -> Boolean.TRUE, ex -> Boolean.FALSE);
  }

  default <R> Result<R> map(Function<? super T, R> success) {
    return flatMap(success::applyFlatten, Result::failed);
  }

  default <R> Result<R> map(Function<? super T, R> success, Function<Exception, R> failure) {
    return flatMap(success::applyFlatten, failure::applyFlatten);
  }

  default Optional<Exception> optionalFailure() {
    return apply(t -> Optional.empty(), Optional::of);
  }

  default Optional<T> optionalSuccess() {
    return apply(Optional::ofNullable, ex -> Optional.empty());
  }

  default Exception reason() {
    return apply(r -> Function.sneakyThrow(new IllegalStateException()), Function.identity());
  }
}
