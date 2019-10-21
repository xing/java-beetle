package com.xing.beetle;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.xing.beetle.testcontainers.ContainerLifecycle;
import com.xing.beetle.testcontainers.ContainerType;

@Testcontainers
@ExtendWith(ContainerLifecycle.class)
class BeetleAcceptanceIT {

  static class Handler extends MessageHandler {

    private final CountDownLatch remaining;
    private final AtomicInteger concurrent;
    private final AtomicInteger max;
    private final java.util.Queue<String> messagesIDs;

    public Handler(int messageCount) {
      this.remaining = new CountDownLatch(messageCount);
      this.concurrent = new AtomicInteger(0);
      this.max = new AtomicInteger(0);
      this.messagesIDs = new ConcurrentLinkedQueue<>();
    }

    synchronized void awaitAllProcessed() throws InterruptedException {
      remaining.await(10, TimeUnit.MINUTES);
    }

    @Override
    public Callable<HandlerResponse> doProcess(
        Envelope envelope, BasicProperties properties, byte[] body) {
      return () -> {
        messagesIDs.add(requireNonNull(properties.getMessageId()));
        max.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
        try {
          Thread.sleep(HANDLER_PROCESSING_IN_MILLIS);
          return HandlerResponse.ok(envelope, properties, body);
        } finally {
          concurrent.getAndDecrement();
          remaining.countDown();
        }
      };
    }
  }

  private static final int MESSAGE_COUNT = 100;
  private static final int HANDLER_PROCESSING_IN_MILLIS = 100;
  private static final double TOLERATED_OVERHEAD = 1.10;
  private static final String TEST_QUEUE_NAME = "testqueue";

  static URI brokerUri(RabbitMQContainer c) {
    return URI.create(
        "amqp://"
            + c.getAdminUsername()
            + ":"
            + c.getAdminPassword()
            + "@"
            + c.getContainerIpAddress()
            + ":"
            + c.getAmqpPort()
            + "/");
  }

  @Container GenericContainer<?> redis = new GenericContainer<>("redis").withExposedPorts(6379);

  @ParameterizedTest(name = "Handlers={0}, Brokers={1}")
  @CsvSource({"1,1", "4,1", "1,2", "4,2"})
  void checkBeetleWith(
      int nHandlerThreads,
      @ContainerType(RabbitMQContainer.class) @ConvertWith(ContainerType.Converter.class)
          RabbitMQContainer[] containers)
      throws Exception {
    Client config = setupClient(nHandlerThreads, containers);
    Handler handler = setupHandler(config, containers);

    try (Client client = config.start()) {
      for (int i = 0; i < MESSAGE_COUNT; i++) {
        client.publish(TEST_QUEUE_NAME, "hello world: " + i);
      }

      Duration expectedRuntime =
          Duration.ofMillis(
              (long)
                  (TOLERATED_OVERHEAD
                      * MESSAGE_COUNT
                      * HANDLER_PROCESSING_IN_MILLIS
                      / nHandlerThreads));
      assertTimeout(expectedRuntime, handler::awaitAllProcessed);
      assertEquals(0, handler.remaining.getCount());
      assertEquals(nHandlerThreads, handler.max.get());
      assertEquals(MESSAGE_COUNT, handler.messagesIDs.size());
    }
  }

  private Client setupClient(int nHandlerThreads, RabbitMQContainer[] containers) {
    ExecutorService executor =
        nHandlerThreads == 1
            ? Executors.newSingleThreadExecutor()
            : Executors.newFixedThreadPool(nHandlerThreads);
    ClientBuilder builder =
        Client.builder()
            .executorService(executor)
            .setDeduplicationStore(
                new RedisConfiguration(redis.getContainerIpAddress(), redis.getMappedPort(6379)));
    Stream.of(containers).map(BeetleAcceptanceIT::brokerUri).forEach(builder::addBroker);
    return builder.build();
  }

  private Handler setupHandler(Client config, RabbitMQContainer[] containers) {
    Handler handler = new Handler(MESSAGE_COUNT);
    config.registerExchange(Exchange.builder().name(TEST_QUEUE_NAME).durable(false).topic(false));
    Queue queue = Queue.builder().name(TEST_QUEUE_NAME).build();
    config.registerQueue(queue);
    config.registerMessage(
        Message.builder().name(TEST_QUEUE_NAME).redundant(containers.length != 1));
    config.registerHandler(new ConsumerConfiguration(queue, handler));
    return handler;
  }
}
