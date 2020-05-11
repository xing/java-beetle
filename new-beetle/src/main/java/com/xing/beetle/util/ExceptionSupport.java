package com.xing.beetle.util;

import java.util.Optional;
import java.util.stream.Stream;

public class ExceptionSupport {

  @FunctionalInterface
  public interface BiConsumer<T, U> extends java.util.function.BiConsumer<T, U> {
    @Override
    default void accept(T t, U u) {
      try {
        acceptChecked(t, u);
      } catch (Throwable e) {
        sneakyThrow(e);
      }
    }

    void acceptChecked(T t, U u) throws Throwable;
  }

  @FunctionalInterface
  public interface Consumer<T> extends java.util.function.Consumer<T> {

    @Override
    default void accept(T element) {
      try {
        acceptChecked(element);
      } catch (Throwable e) {
        sneakyThrow(e);
      }
    }

    void acceptChecked(T element) throws Throwable;

    default Optional<Throwable> executeAndCatch(T element) {
      try {
        acceptChecked(element);
        return Optional.empty();
      } catch (Throwable e) {
        return Optional.of(e);
      }
    }

    default Stream<Throwable> mapAndCatch(Stream<? extends T> elements) {
      return elements.map(this::executeAndCatch).filter(Optional::isPresent).map(Optional::get);
    }

    default void mapAndThrow(Stream<? extends T> elements) {
      Exception[] exceptions = mapAndCatch(elements).toArray(Exception[]::new);
      if (exceptions.length > 0) {
        ExceptionSupport.sneakyThrow(exceptions[0]);
      }
    }
  }

  @FunctionalInterface
  public interface Function<T, R> extends java.util.function.Function<T, R> {

    @Override
    default R apply(T t) {
      try {
        return applyChecked(t);
      } catch (Throwable e) {
        return ExceptionSupport.sneakyThrow(e);
      }
    }

    R applyChecked(T t) throws Throwable;
  }

  @FunctionalInterface
  public interface Supplier<T> extends java.util.function.Supplier<T> {

    default Supplier<T> andThen(Consumer<? super T> init) {
      return () -> {
        T obj = get();
        init.acceptChecked(obj);
        return obj;
      };
    }

    default <X> Supplier<X> andThen(Function<? super T, ? extends X> mapper) {
      return () -> mapper.applyChecked(get());
    }

    @Override
    default T get() {
      try {
        return getChecked();
      } catch (Throwable e) {
        return ExceptionSupport.sneakyThrow(e);
      }
    }

    T getChecked() throws Throwable;
  }

  public interface Runnable extends java.lang.Runnable {

    @Override
    default void run() {
      try {
        runChecked();
      } catch (Throwable e) {
        ExceptionSupport.sneakyThrow(e);
      }
    }

    void runChecked() throws Throwable;
  }

  @SuppressWarnings("unchecked")
  public static <E extends Throwable> E sneakyCast(Throwable exception) {
    return (E) exception;
  }

  public static <E extends Throwable, R> R sneakyThrow(Throwable exception) throws E {
    E casted = sneakyCast(exception);
    throw casted;
  }

  public static <E extends Throwable, R> R sneakyThrow(R result, Throwable exception) throws E {
    if (exception != null) {
      throw ExceptionSupport.<E>sneakyCast(exception);
    } else {
      return result;
    }
  }

  private ExceptionSupport() {}
}
