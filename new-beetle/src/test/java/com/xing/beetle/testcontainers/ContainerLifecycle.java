package com.xing.beetle.testcontainers;

import static java.util.concurrent.CompletableFuture.runAsync;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.testcontainers.lifecycle.Startable;

public class ContainerLifecycle implements InvocationInterceptor {

  private static void doWithContainers(ReflectiveInvocationContext<?> context,
      Consumer<Startable> action) {
    CompletableFuture<?>[] futures =
        context.getArguments().stream().flatMap(ContainerLifecycle::flatten)
            .filter(Startable.class::isInstance).map(Startable.class::cast)
            .map(s -> runAsync(s::start)).toArray(CompletableFuture<?>[]::new);
    CompletableFuture.allOf(futures).join();
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
    doWithContainers(context, Startable::start);
    try {
      invocation.proceed();
    } finally {
      doWithContainers(context, Startable::stop);
    }
  }

  @Override
  public void interceptTestMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
      throws Throwable {
    lifecycled(invocation, invocationContext);
  }

  @Override
  public void interceptTestTemplateMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
      throws Throwable {
    lifecycled(invocation, invocationContext);
  }
}
