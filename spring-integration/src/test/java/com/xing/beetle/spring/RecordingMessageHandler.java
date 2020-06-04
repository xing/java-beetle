package com.xing.beetle.spring;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RecordingMessageHandler {
  protected final System.Logger log = System.getLogger(getClass().getName());
  protected final CopyOnWriteArrayList<String> result = new CopyOnWriteArrayList<>();
  protected final CopyOnWriteArrayList<String> redelivered = new CopyOnWriteArrayList<>();
  protected final CopyOnWriteArrayList<String> deadLettered = new CopyOnWriteArrayList<>();
  protected final CopyOnWriteArrayList<String> queuePolicyMessages = new CopyOnWriteArrayList<>();

  void assertCounts(
      String messageId, long expResult, long expDeadLetter, long expRedelivered, long timeout) {
    CompletableFuture<Long> resultCheck =
        CompletableFuture.supplyAsync(
            () -> {
              long c = 0;
              for (long i = 0; i < timeout; i += 50) {
                c = result.stream().filter(s -> s.equals(messageId)).count();
                if (c == expResult) {
                  return c;
                }
                try {
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  return c;
                }
              }
              return c;
            });

    CompletableFuture<Long> deadLetterCheck =
        CompletableFuture.supplyAsync(
            () -> {
              long c = 0;
              for (long i = 0; i < timeout; i += 50) {
                c = deadLettered.stream().filter(s -> s.equals(messageId)).count();
                if (c == expDeadLetter) {
                  return c;
                }
                try {
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  return c;
                }
              }
              return c;
            });

    CompletableFuture<Long> redeliverCheck =
        CompletableFuture.supplyAsync(
            () -> {
              long c = 0;
              for (long i = 0; i < timeout; i += 50) {
                c = redelivered.stream().filter(s -> s.equals(messageId)).count();
                if (c == expRedelivered) {
                  return c;
                }
                try {
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  return c;
                }
              }
              return c;
            });

    CompletableFuture<Void> combinedFuture =
        CompletableFuture.allOf(resultCheck, redeliverCheck, deadLetterCheck);

    try {
      combinedFuture.get(timeout, TimeUnit.MILLISECONDS);

      assertEquals(expResult, resultCheck.getNow(-1L));
      assertEquals(expDeadLetter, deadLetterCheck.getNow(-1L));
      assertEquals(expRedelivered, redeliverCheck.getNow(-1L));
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      fail(e);
    }
  }
}
