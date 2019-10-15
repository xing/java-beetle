package com.xing.beetle.testcontainers;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.testcontainers.lifecycle.Startable;

public class ContainerLifecycle implements InvocationInterceptor {

  private static Stream<Startable> containers(ReflectiveInvocationContext<?> context) {
    return context
        .getArguments()
        .stream()
        .flatMap(ContainerLifecycle::flatten)
        .filter(Startable.class::isInstance)
        .map(Startable.class::cast);
  }

  private static Stream<?> flatten(Object o) {
    if (o instanceof Stream) {
      return (Stream<?>) o;
    } else if (o instanceof Object[]) {
      return Arrays.stream((Object[]) o);
    } else if (o instanceof Collection) {
      return ((Collection<?>) o).stream();
    } else {
      return Stream.of(o);
    }
  }

  private static void lifecycled(Invocation<?> invocation, ReflectiveInvocationContext<?> context)
      throws Throwable {
    containers(context).forEach(Startable::start);
    try {
      invocation.proceed();
    } finally {
      containers(context).forEach(Startable::stop);
    }
  }

  @Override
  public void interceptTestMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext,
      ExtensionContext extensionContext)
      throws Throwable {
    lifecycled(invocation, invocationContext);
  }

  @Override
  public void interceptTestTemplateMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext,
      ExtensionContext extensionContext)
      throws Throwable {
    lifecycled(invocation, invocationContext);
  }
}
