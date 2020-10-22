package com.xing.beetle;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestContainerProvider {

  private static final Logger logger = Logger.getLogger(TestContainerProvider.class.getName());

  public static String RABBITMQ_VERSION = "rabbitmq:3.8.3";
  public static String REDIS_VERSION = "redis:3.0.2";

  public static List<RabbitMQContainer> rabbitMQContainers;
  public static GenericContainer<?> redis;

  private static boolean started = false;

  public static synchronized void startContainers() {
    if (started) return;

    redis = new GenericContainer<>(DockerImageName.parse(REDIS_VERSION)).withExposedPorts(6379);
    redis.start();

    rabbitMQContainers =
        IntStream.rangeClosed(1, 2)
            .mapToObj(i -> new RabbitMQContainer(RABBITMQ_VERSION).withExposedPorts(5672))
            .collect(Collectors.toList());

    rabbitMQContainers.forEach(GenericContainer::start);

    logger.log(Level.FINE, "Test containers started.");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  redis.stop();
                  rabbitMQContainers.forEach(GenericContainer::stop);
                  logger.log(Level.INFO, "Test containers stopped.");
                }));

    started = true;
  }
}
