package com.xing.beetle.dedup;

import static java.util.Objects.requireNonNull;
import java.util.concurrent.Executor;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.DedupStore;
import com.xing.beetle.dedup.spi.MessageAdapter;

public class MessageHandlingState<K> {

  public interface Outcome<K, M> {

    void apply(MessageAdapter<M, K> adapter, DedupStore<K> store, Executor executor);
  }

  public enum Status {
    INCOMPLETE, FAILED, COMPLETE;
  }

  public static <K> MessageHandlingState<K> initialTry(K key) {
    return new MessageHandlingState<>(key);
  }

  private final K messageKey;
  private final long expires;
  private final Status status;
  private int attempts;
  private int exceptions;

  public MessageHandlingState(K messageKey) {
    this.messageKey = requireNonNull(messageKey);
    this.expires = Integer.MAX_VALUE;
    this.status = Status.INCOMPLETE;
    this.attempts = 0;
    this.exceptions = 0;
  }

  public <M> Outcome<K, M> handle(M message, MessageListener<M> listener) {
    return (adapter, store, executor) -> {
      if (expires < System.currentTimeMillis()) {
        adapter.acknowledge(message);
        listener.onDropped(message);
      } else if (store.tryInsert(this)) {

      } else {
        try {
          attempts++;
          listener.onMessage(message);
        } catch (Throwable e) {
          exceptions++;
          if (listener.handleFailed(e, attempts)) {
            adapter.acknowledge(message);
          } else {
            store.deleteMutex(null);
          }
        }
      }
    };
  }

  public K key() {
    return messageKey;
  }
}
