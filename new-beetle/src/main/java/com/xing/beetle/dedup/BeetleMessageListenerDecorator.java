package com.xing.beetle.dedup;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import com.xing.beetle.dedup.spi.KeyValueStore;
import com.xing.beetle.dedup.spi.MessageAdapter;
import com.xing.beetle.util.Agent;

public class BeetleMessageListenerDecorator<M, K> implements MessageListener<M> {

  private Interruptable<? super M> delegate;
  private Executor executor;
  private Duration timeout;
  private Agent<KeyValueStore<K, MessageHandlingState<K>>> store;
  private Agent<MessageAdapter<M, K>> adapter;

  @Override
  public void onMessage(M message) {
    // CompletionStage<MessageHandlingState<K>> stage = stateOf(message);
    // stage = stage.thenCompose(state -> adapter.perform(a -> state.handleMessage(message, a)));

    // TODOstage = stage.thenApply(state -> state.handleListener(message, delegate));
    // stage = stage.thenCompose(this::updateState); // TODO set mutex
    // stage = stage.thenCompose(
    // state -> delegate.onMessage(message, executor, timeout).handle(state::checkCompletion));
    // stage = stage.thenCompose(this::updateState);

    return;
  }

  // private CompletionStage<MessageHandlingState<K>> stateOf(M message) {
  // return adapter.perform(a -> a.keyOf(message)).thenApply(MessageHandlingState::initialTry)
  // .thenCompose(state -> store.perform(s -> s.tryInsert(state.key(), state))
  // .thenCompose(inserted -> inserted ? CompletableFuture.completedFuture(state)
  // : store.perform(s -> s.retrieve(state.key()))));
  // }

  private CompletionStage<MessageHandlingState<K>> updateState(MessageHandlingState<K> state) {
    return store.perform(s -> s.update(state.key(), state)).thenApply(x -> state);
  }
}
