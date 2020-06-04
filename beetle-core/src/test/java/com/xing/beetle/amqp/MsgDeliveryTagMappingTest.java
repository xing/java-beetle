package com.xing.beetle.amqp;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MsgDeliveryTagMappingTest {

  private MsgDeliveryTagMapping map = new MsgDeliveryTagMapping();

  @Mock private Channel channel;

  @Mock private Consumer callback;

  @Mock private Envelope envelope;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private GetResponse response;

  @Test
  void basicAck() {
    when(envelope.getDeliveryTag()).thenReturn(40L);
    Envelope e = map.envelopeWithPseudoDeliveryTag(channel, envelope);
    assertNotEquals(40L, e.getDeliveryTag());
    assertDoesNotThrow(() -> map.basicAck(e.getDeliveryTag(), false));
    assertDoesNotThrow(() -> verify(channel).basicAck(40L, false));
  }

  @Test
  void basicNack() {
    when(envelope.getDeliveryTag()).thenReturn(41L);
    Envelope e = map.envelopeWithPseudoDeliveryTag(channel, envelope);
    assertNotEquals(41L, e.getDeliveryTag());
    assertDoesNotThrow(() -> map.basicNack(e.getDeliveryTag(), false, false));
    assertDoesNotThrow(() -> verify(channel).basicNack(41L, false, false));
  }

  @Test
  void basicReject() {
    when(envelope.getDeliveryTag()).thenReturn(43L);
    Envelope e = map.envelopeWithPseudoDeliveryTag(channel, envelope);
    assertNotEquals(43L, e.getDeliveryTag());
    assertDoesNotThrow(() -> map.basicReject(e.getDeliveryTag(), false));
    assertDoesNotThrow(() -> verify(channel).basicReject(43L, false));
  }

  @Test
  void createConsumerDecorator() {
    Consumer c = map.createConsumerDecorator(callback, channel);
  }

  @Test
  void mapEnvelope() {
    when(envelope.getDeliveryTag()).thenReturn(42L);
    Envelope e = map.envelopeWithPseudoDeliveryTag(channel, envelope);
    assertNotEquals(42L, e.getDeliveryTag());
  }

  @Test
  void mapResponse() {
    when(response.getEnvelope().getDeliveryTag()).thenReturn(44L);
    GetResponse r = map.responseWithPseudoDeliveryTag(channel, response);
    assertNotEquals(44L, r.getEnvelope().getDeliveryTag());
    assertDoesNotThrow(() -> map.basicAck(r.getEnvelope().getDeliveryTag(), false));
    assertDoesNotThrow(() -> verify(channel).basicAck(44L, false));
  }
}
