package com.xing.beetle.testcontainers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.ArgumentConverter;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.platform.commons.support.ReflectionSupport;
import org.testcontainers.containers.GenericContainer;

@ConvertWith(Containers.ContainerConverter.class)
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Containers {

  class ContainerConverter implements ArgumentConverter {

    private static final TryConverter<String> IMAGE = TryConverter.of(String.class);
    private static final TryConverter<Integer> COUNT =
        TryConverter.of(Number.class)
            .map(Number::intValue)
            .or(TryConverter.of(String.class).map(Integer::valueOf));
    private static final TryConverter<Collector<GenericContainer<?>, ?, ?>> COLLECTOR =
        TryConverter.of(Class.class)
            .<Collector<GenericContainer<?>, ?, ?>>supplyIfMatches(
                List.class::isAssignableFrom, Collectors::toList)
            .or(
                TryConverter.of(Class.class)
                    .<Collector<GenericContainer<?>, ?, ?>>supplyIfMatches(
                        Set.class::isAssignableFrom, Collectors::toSet));

    @SuppressWarnings("unchecked")
    private static <T> T[] newArray(Class<T> componentType, int length) {
      return (T[]) Array.newInstance(componentType, length);
    }

    @SuppressWarnings("rawtypes")
    static GenericContainer<?> newContainer(Class<? extends GenericContainer> type, String image) {
      if (image == null || image.isEmpty()) {
        return ReflectionSupport.newInstance(type);
      } else {
        return ReflectionSupport.newInstance(type, image);
      }
    }

    @SuppressWarnings("rawtypes")
    static Stream<GenericContainer<?>> newContainers(
        int count, Class<? extends GenericContainer> type, String image) {
      return IntStream.range(0, count).mapToObj(i -> newContainer(type, image));
    }

    private static Class<?> rawType(Type type) {
      if (type instanceof Class) {
        return (Class<?>) type;
      } else if (type instanceof ParameterizedType) {
        return rawType(((ParameterizedType) type).getRawType());
      } else {
        throw new IllegalArgumentException(type.getTypeName());
      }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object convert(Object source, ParameterContext context)
        throws ArgumentConversionException {
      Class<?> paramType = context.getParameter().getType();
      String image = context.findAnnotation(Containers.class).map(Containers::image).orElse(null);
      if (paramType.isArray()) {
        Class<? extends GenericContainer> type =
            paramType.getComponentType().asSubclass(GenericContainer.class);
        int count = COUNT.tryConvert(source).get().intValue();
        return newContainers(count, type, image).toArray(len -> newArray(type, len));
      } else if (Collection.class.isAssignableFrom(paramType)) {
        ParameterizedType collectionType =
            (ParameterizedType) context.getParameter().getParameterizedType();
        Class<? extends GenericContainer> type =
            rawType(collectionType.getActualTypeArguments()[0]).asSubclass(GenericContainer.class);
        int count = COUNT.tryConvert(source).get().intValue();
        return newContainers(count, type, image).collect(COLLECTOR.tryConvert(paramType).get());
      } else if (GenericContainer.class.isAssignableFrom(paramType)) {
        String imageSource = IMAGE.tryConvert(source).orElse(image);
        return newContainer(paramType.asSubclass(GenericContainer.class), imageSource);
      } else {
        throw new ArgumentConversionException(String.valueOf(source));
      }
    }
  }

  String image() default "";
}
