package com.xing.beetle.dedup;

import java.util.concurrent.TimeoutException;
import com.xing.beetle.dedup.spi.MessageAdapter;

public class MessageHandlingState<K> {

  public enum Status {
    INCOMPLETE, FAILED, COMPLETE;
  }

  public static <K> MessageHandlingState<K> initialTry(K key) {
    return null;
  }

  private K messageKey;
  private Status status;
  private long timeout;
  private int attempts;
  private int exceptions;

  public boolean acknowledgeMessage() {
    return isComplete() || shouldNotRetry();
  }

  public MessageHandlingState<K> checkCompletion(Object ignored, Throwable exception) {
    attempts++;
    if (exception instanceof TimeoutException) {

    }
    // TODO Auto-generated method stub
    return this;
  }

  public <M> MessageHandlingState<K> handleListener(M message,
      MessageListener<? super M> listener) {
    if (processListener()) {
      listener.onMessage(message);
    }
    // TODO Auto-generated method stub
    return this;
  }


  public <M> MessageHandlingState<K> handleMessage(M message, MessageAdapter<M, K> adapter) {
    if (acknowledgeMessage()) {
      adapter.acknowledge(message);
    } else if (processLater()) {
      adapter.requeue(message);
    }
    // TODO Auto-generated method stub
    return this;
  }

  private boolean isComplete() {
    return status == Status.COMPLETE;
  }

  public K key() {
    return messageKey;
  }

  private boolean processLater() {
    // TODO Auto-generated method stub
    return false;
  }

  private boolean processListener() {
    // TODO Auto-generated method stub
    return false;
  }

  private boolean shouldNotRetry() {
    // TODO Auto-generated method stub
    return false;
  }
}
