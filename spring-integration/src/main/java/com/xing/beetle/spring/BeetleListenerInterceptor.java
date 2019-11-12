package com.xing.beetle.spring;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.Channel;
import com.xing.beetle.dedup.MessageHandlingState;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.KeyValueStore;
import com.xing.beetle.dedup.spi.MessageAdapter;
import com.xing.beetle.util.ExceptionSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;

public class BeetleListenerInterceptor implements MethodInterceptor {

  private static class SpringMessageAdaptor implements MessageAdapter<Message> {

    private final Channel channel;
    private final boolean needToAck;

    public SpringMessageAdaptor(Channel channel, boolean needToAck) {
      this.channel = requireNonNull(channel);
      this.needToAck = needToAck;
    }

    @Override
    public void acknowledge(Message message) {
      if (needToAck) {
        try {
          channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (IOException e) {
          ExceptionSupport.sneakyThrow(e);
        }
      }
    }

    @Override
    public String keyOf(Message message) {
      return message.getMessageProperties().getMessageId();
    }

    @Override
    public void requeue(Message message) {
      if (needToAck) {
        try {
          channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
        } catch (IOException e) {
          ExceptionSupport.sneakyThrow(e);
        }
      }
    }
  }

  private final KeyValueStore<String> store;
  private final Map<String, AcknowledgeMode> acknowledgeModes;

  public BeetleListenerInterceptor(
      KeyValueStore<String> store, RabbitListenerEndpointRegistry registry) {
    this.store = requireNonNull(store);
    this.acknowledgeModes =
        registry.getListenerContainers().stream()
            .filter(AbstractMessageListenerContainer.class::isInstance)
            .map(AbstractMessageListenerContainer.class::cast)
            .flatMap(c -> Stream.of(c.getQueueNames()).map(q -> Map.entry(q, c)))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAcknowledgeMode()));
  }

  private MessageAdapter<Message> adapter(Channel channel, Message message) {
    String queue = message.getMessageProperties().getConsumerQueue();
    AcknowledgeMode mode = acknowledgeModes.getOrDefault(queue, AcknowledgeMode.AUTO);
    return new SpringMessageAdaptor(channel, mode == AcknowledgeMode.MANUAL);
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Channel channel = (Channel) invocation.getArguments()[0];
    Object data = invocation.getArguments()[1];
    boolean multiple = data instanceof List;
    @SuppressWarnings("unchecked")
    List<Message> messages =
        multiple
            ? (List<Message>) data
            : new ArrayList<>(Collections.singletonList((Message) data));
    MessageListener<Message> listener =
        msg -> {
          invocation.getArguments()[1] = multiple ? Collections.singletonList(msg) : msg;
          invocation.proceed();
        };
    KeyValueStore<MessageHandlingState.Status> statusStore =
        store.suffixed(
            "status", MessageHandlingState.Status::valueOf, MessageHandlingState.Status::toString);
    messages.forEach(
        msg ->
            statusStore
                .getNullable(
                    msg.getMessageProperties().getMessageId(),
                    MessageHandlingState.Status.NONREDUNDANT)
                .orElse(MessageHandlingState.Status.INCOMPLETE)
                .handle(msg, listener)
                .apply(adapter(channel, msg), store, null));
    return null;
  }
}
