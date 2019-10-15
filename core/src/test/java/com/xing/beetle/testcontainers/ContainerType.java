package com.xing.beetle.testcontainers;

import static java.util.Objects.requireNonNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.util.stream.IntStream;

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.ArgumentConverter;
import org.junit.platform.commons.support.ReflectionSupport;
import org.testcontainers.containers.GenericContainer;

@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface ContainerType {

  class Converter implements ArgumentConverter {

    static GenericContainer<?>[] createContainers(int count, ContainerType type) {
      return IntStream.range(0, count)
          .mapToObj(i -> newContainer(type))
          .toArray(len -> newArray(type.value(), len));
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] newArray(Class<T> componentType, int length) {
      return (T[]) Array.newInstance(componentType, length);
    }

    static GenericContainer<?> newContainer(ContainerType type) {
      if (type.imageName().isEmpty()) {
        return ReflectionSupport.newInstance(type.value());
      } else {
        return ReflectionSupport.newInstance(type.value(), type.imageName());
      }
    }

    @Override
    public GenericContainer<?>[] convert(Object source, ParameterContext context)
        throws ArgumentConversionException {
      ContainerType containerType =
          requireNonNull(context.getParameter().getAnnotation(ContainerType.class));
      if (source instanceof Number) {
        return createContainers(((Number) source).intValue(), containerType);
      } else if (source instanceof String) {
        return createContainers(Integer.parseInt((String) source), containerType);
      } else {
        throw new ArgumentConversionException(String.valueOf(source));
      }
    }
  }

  String imageName() default "";

  @SuppressWarnings("rawtypes")
  Class<? extends GenericContainer> value() default GenericContainer.class;
}
