package com.xing.beetle.spring;

import com.rabbitmq.client.Channel;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.Deduplicator;
import com.xing.beetle.dedup.spi.MessageAdapter;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class BeetleListenerInterceptor implements MethodInterceptor {

  private final Deduplicator store;
  private final RabbitListenerEndpointRegistry registry;
  private final boolean rejectAndRequeue;
  private Map<String, AcknowledgeMode> acknowledgeModes;

  public BeetleListenerInterceptor(
      Deduplicator store, RabbitListenerEndpointRegistry registry, boolean rejectAndRequeue) {
    this.store = requireNonNull(store);
    this.registry = requireNonNull(registry);
    this.rejectAndRequeue = rejectAndRequeue;
  }

  @EventListener
  void onApplicationStarted(ContextRefreshedEvent event) {
    ackModes();
  }

  private Map<String, AcknowledgeMode> ackModes() {
    if (acknowledgeModes != null) return acknowledgeModes;
    acknowledgeModes =
        registry.getListenerContainers().stream()
            .filter(AbstractMessageListenerContainer.class::isInstance)
            .map(AbstractMessageListenerContainer.class::cast)
            .flatMap(c -> Stream.of(c.getListenerId()).map(q -> Map.entry(q, c)))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAcknowledgeMode()));
    return acknowledgeModes;
  }

  private MessageAdapter<Message> adapter(Channel channel, Message message) {
    String queue = message.getMessageProperties().getConsumerQueue();
    AcknowledgeMode mode = ackModes().getOrDefault(queue, AcknowledgeMode.AUTO);
    return new SpringMessageAdapter(channel, mode == AcknowledgeMode.AUTO, rejectAndRequeue);
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
    messages.forEach(
        msg ->
            store.handle(
                msg,
                msg.getMessageProperties().getConsumerQueue(),
                adapter(channel, msg),
                listener));
    return null;
  }
}
