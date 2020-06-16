package com.xing.beetle.spring;

import com.rabbitmq.client.Channel;
import com.xing.beetle.dedup.spi.MessageAdapter;
import com.xing.beetle.util.ExceptionSupport;
import org.springframework.amqp.core.Message;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

class SpringMessageAdapter implements MessageAdapter<Message> {

  private final Channel channel;
  private final boolean needToAck;
  private final boolean rejectAndRequeue;
  private static final int FLAG_REDUNDANT = 1;

  SpringMessageAdapter(Channel channel, boolean needToAck, boolean rejectAndRequeue) {
    this.channel = requireNonNull(channel);
    this.needToAck = needToAck;
    this.rejectAndRequeue = rejectAndRequeue;
  }

  @Override
  public void drop(Message message) {
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
  public long expiresAt(Message message) {
    Object expiresAt = message.getMessageProperties().getHeader("expires_at");
    if (expiresAt == null) {
      return Long.MAX_VALUE;
    } else if (expiresAt instanceof Number) {
      return ((Number) expiresAt).longValue();
    } else if (expiresAt instanceof String) {
      return Long.parseLong((String) expiresAt);
    } else {
      throw new IllegalArgumentException(
          "Unexpected expires_at header value " + expiresAt.getClass());
    }
  }

  @Override
  public boolean isRedundant(Message message) {
    Object flags = message.getMessageProperties().getHeader("flags");
    if (flags == null) {
      return false;
    } else if (flags instanceof Number) {
      return ((Number) flags).intValue() == FLAG_REDUNDANT;
    } else if (flags instanceof String) {
      return Integer.parseInt((String) flags) == FLAG_REDUNDANT;
    } else {
      throw new IllegalArgumentException("Unexpected flags header value " + flags.getClass());
    }
  }

  @Override
  public void requeue(Message message) {
    if (needToAck) {
      try {
        channel.basicReject(message.getMessageProperties().getDeliveryTag(), rejectAndRequeue);
      } catch (IOException e) {
        ExceptionSupport.sneakyThrow(e);
      }
    }
  }
}
