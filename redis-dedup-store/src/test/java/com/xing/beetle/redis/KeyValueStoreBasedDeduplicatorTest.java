package com.xing.beetle.redis;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.dedup.spi.KeyValueStoreBasedDeduplicator;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.GenericContainer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/** Integration test for KeyValueStoreBasedDeduplicator with a Redis container. */
@ExtendWith(MockitoExtension.class)
class KeyValueStoreBasedDeduplicatorTest {

  private static String redisServer = "";

  static {
    GenericContainer redis = startRedisContainer();
    redisServer = getRedisAddress(redis);
  }

  @Mock BeetleAmqpConfiguration beetleAmqpConfiguration;

  @NotNull
  private static String getRedisAddress(GenericContainer redisContainer) {
    return String.join(
        ":",
        new String[] {
          redisContainer.getContainerIpAddress(), redisContainer.getFirstMappedPort() + ""
        });
  }

  @NotNull
  private static GenericContainer startRedisContainer() {
    GenericContainer localRedis = new GenericContainer("redis:3.0.2").withExposedPorts(6379);
    localRedis.start();
    return localRedis;
  }

  @Test
  void testBasicOperations() throws InterruptedException {
    when(beetleAmqpConfiguration.getBeetleRedisServer()).thenReturn(redisServer);
    when(beetleAmqpConfiguration.getRedisFailoverTimeout()).thenReturn(3);

    RedisDedupStore store = new RedisDedupStore(beetleAmqpConfiguration);
    KeyValueStoreBasedDeduplicator deduplicator =
        new KeyValueStoreBasedDeduplicator(store, beetleAmqpConfiguration);
    assertFalse(deduplicator.delayed("messageId"));
    deduplicator.setDelay("messageId", System.currentTimeMillis() + 1000);
    assertTrue(deduplicator.delayed("messageId"));
    Thread.sleep(1100);
    assertFalse(deduplicator.delayed("messageId"));

    assertEquals(1, deduplicator.incrementAckCount("messageId"));
    assertEquals(1, deduplicator.incrementAttempts("messageId"));
    assertEquals(1, deduplicator.incrementExceptions("messageId"));
  }

  @Test
  void testTryAcquireMutex() throws InterruptedException {
    when(beetleAmqpConfiguration.getBeetleRedisServer()).thenReturn(redisServer);
    when(beetleAmqpConfiguration.getRedisFailoverTimeout()).thenReturn(3);

    RedisDedupStore store = new RedisDedupStore(beetleAmqpConfiguration);
    KeyValueStoreBasedDeduplicator deduplicator =
        new KeyValueStoreBasedDeduplicator(store, beetleAmqpConfiguration);
    assertTrue(deduplicator.tryAcquireMutex("messageId", 1));
    assertFalse(deduplicator.tryAcquireMutex("messageId", 1));
    Thread.sleep(1100);
    assertTrue(deduplicator.tryAcquireMutex("messageId", 1));
    deduplicator.releaseMutex("messageId");
    assertTrue(deduplicator.tryAcquireMutex("messageId", 1));
    deduplicator.releaseMutex("messageId");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 5})
  void testDeleteKeys(int expiryInterval) throws InterruptedException {
    when(beetleAmqpConfiguration.getBeetleRedisServer()).thenReturn(redisServer);
    when(beetleAmqpConfiguration.getRedisFailoverTimeout()).thenReturn(3);
    when(beetleAmqpConfiguration.getBeetleRedisStatusKeyExpiryInterval())
        .thenReturn(expiryInterval);

    RedisDedupStore store = new RedisDedupStore(beetleAmqpConfiguration);
    KeyValueStoreBasedDeduplicator deduplicator =
        new KeyValueStoreBasedDeduplicator(store, beetleAmqpConfiguration);
    assertFalse(deduplicator.completed("messageId"));
    deduplicator.complete("messageId");
    assertTrue(deduplicator.completed("messageId"));
    deduplicator.deleteKeys("messageId");
    boolean statusExists = store.get("messageId:status").isPresent();

    if (expiryInterval > 0) {
      assertTrue(statusExists);
    } else {
      assertFalse(statusExists);
    }
  }
}
