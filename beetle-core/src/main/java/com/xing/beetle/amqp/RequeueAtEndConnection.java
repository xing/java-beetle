package com.xing.beetle.amqp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.xing.beetle.util.ExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.util.Objects.requireNonNull;

public class RequeueAtEndConnection implements DefaultConnection.Decorator {

  private static final Logger log = LoggerFactory.getLogger(RequeueAtEndConnection.class);
  private ObjectMapper objectMapper = new ObjectMapper();

  private class RequeueAtEndChannel implements DefaultChannel.Decorator {

    private static final String DEAD_LETTER_SUFFIX = "_dead_letter";

    private final Channel delegate;
    private final SortedSet<Long> deadLetterDeliveryTags;

    RequeueAtEndChannel(Channel delegate) {
      this.delegate = requireNonNull(delegate);
      this.deadLetterDeliveryTags = new ConcurrentSkipListSet<>();
    }

    @Override
    public String basicConsume(
        String queue,
        boolean autoAck,
        String consumerTag,
        boolean noLocal,
        boolean exclusive,
        Map<String, Object> arguments,
        Consumer callback)
        throws IOException {
      Consumer consumer =
          autoAck
              ? callback
              : new Consumer() {

                @Override
                public void handleCancel(String consumerTag) throws IOException {
                  callback.handleCancel(consumerTag);
                }

                @Override
                public void handleCancelOk(String consumerTag) {
                  callback.handleCancelOk(consumerTag);
                }

                @Override
                public void handleConsumeOk(String consumerTag) {
                  callback.handleConsumeOk(consumerTag);
                }

                @Override
                public void handleDelivery(
                    String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                  envelope = deadLetterCheck(queue, envelope, properties.getHeaders());
                  callback.handleDelivery(consumerTag, envelope, properties, body);
                }

                @Override
                public void handleRecoverOk(String consumerTag) {
                  callback.handleRecoverOk(consumerTag);
                }

                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                  callback.handleShutdownSignal(consumerTag, sig);
                }
              };
      return delegate.basicConsume(
          queue, autoAck, consumerTag, noLocal, exclusive, arguments, consumer);
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
      GetResponse response = delegate.basicGet(queue, autoAck);
      if (!autoAck && response != null) {
        Envelope envelope =
            deadLetterCheck(queue, response.getEnvelope(), response.getProps().getHeaders());
        response =
            new GetResponse(
                envelope, response.getProps(), response.getBody(), response.getMessageCount());
      }
      return response;
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
      boolean deadLettered = deadLettered(deliveryTag, multiple);
      if (deadLettered) {
        if (requeue) {
          // reject to dead letter queue
          delegate.basicNack(deliveryTag, multiple, false);
        } else {
          // silently drop the message by accepting
          delegate.basicAck(deliveryTag, multiple);
        }
      } else {
        delegate.basicNack(deliveryTag, multiple, requeue);
      }
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
      boolean deadLettered = deadLettered(deliveryTag, false);
      if (deadLettered) {
        if (requeue) {
          // reject to dead letter queue
          delegate.basicReject(deliveryTag, false);
        } else {
          // silently drop the message by accepting
          log.warn("message with delivery tag silently dropped " + deliveryTag);
          delegate.basicAck(deliveryTag, false);
        }
      } else {
        delegate.basicReject(deliveryTag, requeue);
      }
    }

    private Map<String, Object> configureDeadLetter(String queue, long ttlInMillis) {
      Map<String, Object> arguments = new HashMap<>();
      arguments.put("x-dead-letter-exchange", "");
      arguments.put("x-dead-letter-routing-key", queue);
      arguments.put("x-message-ttl", ttlInMillis);
      return arguments;
    }

    private Map<String, Object> configureOriginal(Map<String, Object> arguments, String queue) {
      arguments = new HashMap<>(arguments != null ? arguments : Collections.emptyMap());
      arguments.put("x-dead-letter-exchange", "");
      arguments.put("x-dead-letter-routing-key", queue + DEAD_LETTER_SUFFIX);
      return arguments;
    }

    private Envelope deadLetterCheck(String queue, Envelope envelope, Map<String, Object> headers) {
      if (deadLetterQueues.contains(queue)) {
        deadLetterDeliveryTags.add(envelope.getDeliveryTag());
        if (headers != null && headers.containsKey("x-first-death-reason")) {
          envelope =
              new Envelope(
                  envelope.getDeliveryTag(),
                  true,
                  envelope.getExchange(),
                  envelope.getRoutingKey());
        }
      }
      return envelope;
    }

    private boolean deadLettered(long deliveryTag, boolean multiple) {
      boolean deadLettered = deadLetterDeliveryTags.contains(deliveryTag);
      if (multiple) {
        deadLetterDeliveryTags.headSet(deliveryTag + 1).clear();
      } else {
        deadLetterDeliveryTags.remove(deliveryTag);
      }
      return deadLettered;
    }

    @Override
    public <R> R delegateMap(Type type, ExceptionSupport.Function<Channel, ? extends R> ch) {
      return ch.apply(delegate);
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare(
        String queue,
        boolean durable,
        boolean exclusive,
        boolean autoDelete,
        Map<String, Object> arguments)
        throws IOException {

      if (beetleAmqpConfiguration.isDeadLetteringEnabled()) {
        arguments = configureOriginal(arguments, queue);
        Map<String, Object> deadLetterArgs =
            configureDeadLetter(queue, beetleAmqpConfiguration.getDeadLetteringMsgTtlMs());
        AMQP.Queue.DeclareOk ok =
            delegate.queueDeclare(
                queue + DEAD_LETTER_SUFFIX, durable, exclusive, autoDelete, deadLetterArgs);
        if (ok.getQueue() == null || ok.getQueue().isEmpty()) {
          return ok;
        }
        queueDeclared(queue);
      }

      publishPolicyOptions(queue);
      return delegate.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }

    void publishPolicyOptions(String queue) throws IOException {
      PolicyUpdatePayload payload = new PolicyUpdatePayload();
      payload.setServer(delegate.getConnection().getAddress().toString());
      payload.setQueue_name(queue);
      if (beetleAmqpConfiguration.isDeadLetteringEnabled()) {
        payload.setDead_letter_queue_name(queue + DEAD_LETTER_SUFFIX);
      }
      PolicyUpdatePayload.Bindings bindings = payload.new Bindings();
      bindings.setExchange(queue);
      bindings.setKey(queue);
      payload.setBindings(bindings);
      payload.setDead_lettering(beetleAmqpConfiguration.isDeadLetteringEnabled());
      payload.setLazy(beetleAmqpConfiguration.isLazyQueuesEnabled());
      payload.setMessage_ttl(beetleAmqpConfiguration.getDeadLetteringMsgTtlMs());

      log.debug(
          "Beetle: publishing policy options on {}: {}",
          payload.getServer(),
          objectMapper.writeValueAsString(payload));
      delegate.exchangeDeclare(
          beetleAmqpConfiguration.getBeetlePolicyExchangeName(), BuiltinExchangeType.TOPIC, true);
      delegate.queueDeclare(
          beetleAmqpConfiguration.getBeetlePolicyUpdatesQueueName(), true, false, false, null);
      delegate.queueBind(
          beetleAmqpConfiguration.getBeetlePolicyUpdatesQueueName(),
          beetleAmqpConfiguration.getBeetlePolicyExchangeName(),
          beetleAmqpConfiguration.getBeetlePolicyUpdatesRoutingKey());
      delegate.basicPublish(
          beetleAmqpConfiguration.getBeetlePolicyExchangeName(),
          beetleAmqpConfiguration.getBeetlePolicyUpdatesRoutingKey(),
          null,
          objectMapper.writeValueAsString(payload).getBytes());
    }
  }

  private final Connection delegate;
  private final boolean invertRequeueParameter;
  private final Set<String> deadLetterQueues;
  private final BeetleAmqpConfiguration beetleAmqpConfiguration;

  public RequeueAtEndConnection(
      Connection delegate,
      BeetleAmqpConfiguration beetleAmqpConfiguration,
      boolean invertRequeueParameter) {
    this.beetleAmqpConfiguration = beetleAmqpConfiguration;
    this.delegate = requireNonNull(delegate);
    this.invertRequeueParameter = invertRequeueParameter;
    this.deadLetterQueues = new HashSet<>();
  }

  private void queueDeclared(String queue) {
    if (invertRequeueParameter) {
      deadLetterQueues.add(queue);
    }
  }

  @Override
  public Channel createChannel(int channelNumber) throws IOException {
    Channel channel =
        channelNumber >= 0 ? delegate.createChannel(channelNumber) : delegate.createChannel();
    return new RequeueAtEndChannel(channel);
  }

  @Override
  public <R> R delegateMap(ExceptionSupport.Function<Connection, ? extends R> con) {
    return con.apply(delegate);
  }
}
