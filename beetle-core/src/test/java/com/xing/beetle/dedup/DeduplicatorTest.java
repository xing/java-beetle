package com.xing.beetle.dedup;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.Deduplicator;
import com.xing.beetle.dedup.spi.KeyValueStore;
import com.xing.beetle.dedup.spi.KeyValueStoreBasedDeduplicator;
import com.xing.beetle.dedup.spi.MessageAdapter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class DeduplicatorTest {

  static class TestMessage {}

  @Mock private MessageAdapter<TestMessage> messageAdapter;

  @Mock private MessageListener<TestMessage> messageListener;

  @Mock private KeyValueStore keyValueStore;

  @Test
  public void test_redundantMessagesProcessedOnceSuccessfully() throws Throwable {

    MockitoAnnotations.initMocks(this);

    TestMessage message = new TestMessage();
    when(messageAdapter.isRedundant(any())).thenReturn(true);
    when(messageAdapter.keyOf(eq(message))).thenReturn("msgid:queue:test-message-id");
    when(keyValueStore.putIfAbsentTtl(
            eq("msgid:queue:test-message-id:mutex"), any(KeyValueStore.Value.class), anyInt()))
        .thenReturn(true);
    when(keyValueStore.putIfAbsentTtl(
            eq("msgid:queue:test-message-id:status"), any(KeyValueStore.Value.class), anyInt()))
        .thenReturn(true);

    Deduplicator deduplicator =
        new KeyValueStoreBasedDeduplicator(keyValueStore, new BeetleAmqpConfiguration());
    deduplicator.handle(message, messageAdapter, messageListener);

    ArgumentCaptor<KeyValueStore.Value> captor = ArgumentCaptor.forClass(KeyValueStore.Value.class);
    verify(keyValueStore, times(1)).put(eq("msgid:queue:test-message-id:status"), captor.capture());
    assertEquals("completed", captor.getValue().getAsString());

    // mutex should be released after completion
    verify(keyValueStore).delete(eq("msgid:queue:test-message-id:mutex"));
    verify(keyValueStore, times(1)).increase(eq("msgid:queue:test-message-id:ack_count"));

    when(keyValueStore.get(eq("msgid:queue:test-message-id:status")))
        .thenReturn(java.util.Optional.of(new KeyValueStore.Value("completed")));
    when(keyValueStore.increase(eq("msgid:queue:test-message-id:ack_count"))).thenReturn(2L);
    // same (redundant) message which was completed is received by deduplicator again
    deduplicator.handle(message, messageAdapter, messageListener);

    // we should get 2 ACKs in total, 1 for each message
    verify(keyValueStore, times(2)).increase(eq("msgid:queue:test-message-id:ack_count"));

    // verify that the message is processed only once
    verify(messageListener, times(1)).onMessage(eq(message));

    verifyKeyCleanUp();
  }

  public void verifyKeyCleanUp() {
    ArgumentCaptor<String> varargCaptor = ArgumentCaptor.forClass(String.class);
    // verify cleaning up  keys
    verify(keyValueStore).deleteMultiple(varargCaptor.capture());
    assertArrayEquals(
        Arrays.stream(Deduplicator.keySuffixes)
            .map(s -> "msgid:queue:test-message-id:" + s)
            .toArray(),
        varargCaptor.getAllValues().toArray());
  }

  @Captor private ArgumentCaptor<Map<String, KeyValueStore.Value>> argCaptor;

  @Test
  public void test_redundantMessageFail_shouldBeRequeued() throws Throwable {

    MockitoAnnotations.initMocks(this);

    TestMessage message = new TestMessage();
    when(messageAdapter.isRedundant(any())).thenReturn(true);
    when(messageAdapter.keyOf(eq(message))).thenReturn("msgid:queue:test-message-id");
    when(keyValueStore.putIfAbsentTtl(
            eq("msgid:queue:test-message-id:mutex"), any(KeyValueStore.Value.class), anyInt()))
        .thenReturn(true);
    when(keyValueStore.putIfAbsent(anyMap())).thenReturn(true);

    doThrow(new InterruptedException()).when(messageListener).onMessage(message);

    BeetleAmqpConfiguration beetleAmqpConfig = new BeetleAmqpConfiguration();
    beetleAmqpConfig.setMaxHandlerExecutionAttempts(3);
    beetleAmqpConfig.setExceptionLimit(3);
    Deduplicator deduplicator = new KeyValueStoreBasedDeduplicator(keyValueStore, beetleAmqpConfig);
    deduplicator.handle(message, messageAdapter, messageListener);

    verify(keyValueStore, times(1)).putIfAbsent(argCaptor.capture());

    Map<String, KeyValueStore.Value> initArgs = argCaptor.getValue();
    assertEquals("incomplete", initArgs.get("msgid:queue:test-message-id:status").getAsString());
    assertTrue(initArgs.containsKey("msgid:queue:test-message-id:expires"));

    verify(keyValueStore, times(1)).increase(eq("msgid:queue:test-message-id:exceptions"));
    verify(keyValueStore, times(1)).increase(eq("msgid:queue:test-message-id:attempts"));
    verify(keyValueStore, times(1)).put(eq("msgid:queue:test-message-id:delay"), any());
    verify(messageAdapter, times(1)).requeue(message);

    // mutex should be released after completion
    verify(keyValueStore).delete(eq("msgid:queue:test-message-id:mutex"));
  }
}
