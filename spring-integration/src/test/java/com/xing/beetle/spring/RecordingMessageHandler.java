package com.xing.beetle.spring;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.*;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordingMessageHandler {
  protected final System.Logger log = System.getLogger(getClass().getName());
  protected final CopyOnWriteArrayList<String> result = new CopyOnWriteArrayList<>();
  protected final CopyOnWriteArrayList<String> redelivered = new CopyOnWriteArrayList<>();
  protected final CopyOnWriteArrayList<String> deadLettered = new CopyOnWriteArrayList<>();
  protected final CopyOnWriteArrayList<String> queuePolicyMessages = new CopyOnWriteArrayList<>();

  void assertCounts(
      String messageId, long expResult, long expDeadLetter, long expRedelivered, long timeout) {
    CompletableFuture<Long> resultCheck =
        CompletableFuture.supplyAsync(countEventually(result, messageId, expResult, timeout));

    CompletableFuture<Long> deadLetterCheck =
        CompletableFuture.supplyAsync(
            countEventually(deadLettered, messageId, expDeadLetter, timeout));

    CompletableFuture<Long> redeliverCheck =
        CompletableFuture.supplyAsync(
            countEventually(redelivered, messageId, expRedelivered, timeout));

    CompletableFuture<Void> combinedFuture =
        CompletableFuture.allOf(resultCheck, redeliverCheck, deadLetterCheck);

    try {
      combinedFuture.get(timeout, TimeUnit.MILLISECONDS);

      assertEquals(expResult, resultCheck.getNow(0L));
      assertEquals(expDeadLetter, deadLetterCheck.getNow(0L));
      assertEquals(expRedelivered, redeliverCheck.getNow(0L));
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      assertEquals(expResult, resultCheck.getNow(0L));
      assertEquals(expDeadLetter, deadLetterCheck.getNow(0L));
      assertEquals(expRedelivered, redeliverCheck.getNow(0L));
      // fail(e);
    }
  }

  /**
   * filters the list for the expected message ID, counts the occurrences and compares it to the
   * expected count until it matches or the timeout is reached.
   *
   * @param messageList list with message IDs
   * @param messageId ID to look for
   * @param expResult expected number of occurrences
   * @param timeout maximum number of milliseconds to retry in total
   * @return final count
   */
  @NotNull
  private Supplier<Long> countEventually(
      CopyOnWriteArrayList<String> messageList, String messageId, long expResult, long timeout) {
    return () -> {
      long c = 0;
      for (long i = 0; i < timeout; i += 50) {
        c = messageList.stream().filter(s -> s.equals(messageId)).count();
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
    };
  }
}
