package com.xing.beetle;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Assertions {

  public static <T> void assertEventualLength(List<T> a, int len, long timeout) throws Exception {
    CountDownLatch l = new CountDownLatch(1);

    new Thread(
            new Runnable() {
              @Override
              public void run() {
                for (; ; ) {
                  if (a.size() == len) {
                    l.countDown();
                  }
                  try {
                    if (l.await(5, TimeUnit.MILLISECONDS)) {
                      return;
                    }
                  } catch (InterruptedException e) {
                    return;
                  }
                }
              }
            })
        .start();

    if (l.await(timeout, TimeUnit.MILLISECONDS)) {
      assertEquals(len, a.size());
      return;
    }

    l.countDown();
    assertEquals(len, a.size());
  }
}
