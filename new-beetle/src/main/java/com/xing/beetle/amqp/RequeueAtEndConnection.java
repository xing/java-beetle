package com.xing.beetle.amqp;

import static java.util.Objects.requireNonNull;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.xing.beetle.BeetleHeader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RequeueAtEndConnection implements ConnectionDecorator.Single {

  private class RequeueAtEndChannel implements ChannelDecorator.Single {

    private static final String DEAD_LETTER_SUFFIX = "_dead_letter";

    private final Channel delegate;

    public RequeueAtEndChannel(Channel delegate) {
      this.delegate = requireNonNull(delegate);
    }

    @Override
    public Channel delegate() {
      return delegate;
    }

    private Map<String, Object> configureDeadLetter(String queue, long ttlInMillis) {
      Map<String, Object> arguments = new HashMap<>();
      arguments.put("x-dead-letter-exchange", "");
      arguments.put("x-dead-letter-routing-key", queue);
      arguments.put("x-message-ttl", ttlInMillis);
      return arguments;
    }

    private Map<String, Object> configureOriginal(Map<String, Object> arguments, String queue) {
      arguments = new HashMap<>(arguments);
      arguments.remove(BeetleHeader.REQUEUE_AT_END_DELAY);
      arguments.put("x-dead-letter-exchange", "");
      arguments.put("x-dead-letter-routing-key", queue + DEAD_LETTER_SUFFIX);
      return arguments;
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare(
        String queue,
        boolean durable,
        boolean exclusive,
        boolean autoDelete,
        Map<String, Object> arguments)
        throws IOException {
      long ttlInMillis = requeueAtEndDelayInMillis;
      if (arguments != null) {
        ttlInMillis = (long) arguments.getOrDefault(BeetleHeader.REQUEUE_AT_END_DELAY, ttlInMillis);
      }
      if (ttlInMillis >= 0) {
        arguments = configureOriginal(arguments, queue);
        Map<String, Object> deadLetterArgs = configureDeadLetter(queue, ttlInMillis);
        AMQP.Queue.DeclareOk ok =
            delegate.queueDeclare(
                queue + DEAD_LETTER_SUFFIX, durable, exclusive, autoDelete, deadLetterArgs);
        if (ok.getQueue() == null || ok.getQueue().isEmpty()) {
          return ok;
        }
      }
      return delegate.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }
  }

  private final Connection delegate;
  private final long requeueAtEndDelayInMillis;

  public RequeueAtEndConnection(Connection delegate) {
    this(delegate, -1);
  }

  public RequeueAtEndConnection(Connection delegate, long requeueAtEndDelayInMillis) {
    this.delegate = requireNonNull(delegate);
    this.requeueAtEndDelayInMillis = requeueAtEndDelayInMillis;
  }

  @Override
  public Connection delegate() {
    return delegate;
  }

  @Override
  public Channel createChannel(int channelNumber) throws IOException {
    Channel channel =
        channelNumber >= 0 ? delegate.createChannel(channelNumber) : delegate.createChannel();
    return new RequeueAtEndChannel(channel);
  }
}
