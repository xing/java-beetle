package com.xing.beetle.redis;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Failover {

  private static Logger logger = LoggerFactory.getLogger(Failover.class);
  private final int timeout;
  private final int maxRetries;

  Failover(int timeout, int maxRetries) {
    this.timeout = timeout;
    this.maxRetries = maxRetries;
  }

  interface ThrowingSupplier<Out> {
    Out supply() throws Exception;
  }

  <Out> Optional<Out> execute(ThrowingSupplier<Out> attempt) {

    Supplier<Optional<Out>> catchingSupplier =
        () -> {
          try {
            return Optional.ofNullable(attempt.supply());
          } catch (Exception e) {
            logger.warn("Redis request {} to {} failed: ", attempt, "");
            sleep(1);
            logger.info("Retrying redis operation {} to:  {} ", attempt, "");
            return Optional.empty();
          }
        };

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    Future<Optional<Out>> future =
        executorService.submit(
            () -> {
              return Stream.iterate(catchingSupplier, i -> i)
                  .limit(maxRetries)
                  .map(Supplier::get)
                  .filter(Optional::isPresent)
                  .findFirst()
                  .flatMap(Function.identity());
            });

    try {
      return future.get(timeout, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      logger.warn("Redis request timed out");
      throw new DeduplicationException("Request timed out", e);
    } catch (Exception e) {
      throw new DeduplicationException("Request failed", e);
    }
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
