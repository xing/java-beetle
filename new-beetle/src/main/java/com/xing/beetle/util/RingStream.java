package com.xing.beetle.util;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class RingStream<E> implements Iterable<E>, Iterator<E> {

  private final E[] elements;
  private final AtomicInteger current;

  @SafeVarargs
  public RingStream(E... elements) {
    this.elements = requireNonNull(elements);
    this.current = new AtomicInteger();
  }

  @Override
  public boolean hasNext() {
    return elements.length != 0;
  }

  @Override
  public Iterator<E> iterator() {
    return this;
  }

  @Override
  public E next() {
    int index = current.getAndUpdate(v -> (v + 1) % elements.length);
    return elements[index];
  }

  public int size() {
    return elements.length;
  }

  public Stream<E> streamAll() {
    return streamLimited(size());
  }

  public Stream<E> streamLimited(int count) {
    return streamUnlimited().limit(count);
  }

  public Stream<E> streamUnlimited() {
    return Stream.generate(this::next).sequential();
  }

  @SafeVarargs
  public final RingStream<E> with(E... others) {
    E[] copy = Arrays.copyOf(elements, elements.length + others.length);
    System.arraycopy(others, 0, copy, elements.length, others.length);
    return new RingStream<>(copy);
  }
}
