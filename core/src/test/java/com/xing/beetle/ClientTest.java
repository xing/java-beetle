package com.xing.beetle;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;
import org.mockito.stubbing.OngoingStubbing;

public class ClientTest {

  @Test
  public void clientStartStartsConsumers() throws Exception {
    final Client client = Client.builder().build();
    final Client clientSpy = spy(client);

    final Channel channel = mockedChannelForClient(clientSpy);

    MessageHandler handler =
        new MessageHandler() {
          @Override
          public Callable<HandlerResponse> doProcess(
              Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            return () -> HandlerResponse.ok(envelope, properties, body);
          }
        };

    Queue queue = Queue.builder().name("test-queue").build();
    clientSpy.registerQueue(queue);
    clientSpy.registerHandler(new ConsumerConfiguration(queue, handler));
    clientSpy.start();
    verify(channel).basicConsume(eq("test-queue"), Matchers.<Consumer>anyObject());
  }

  @Test
  public void connectDeclaresAMQPItems() throws Exception {
    Client client = Client.builder().addBroker(1234).build();

    final ConnectionFactory mockFactory = mock(ConnectionFactory.class);
    final Connection mockConnection = mock(Connection.class);
    when(mockFactory.newConnection()).thenReturn(mockConnection);

    final Channel mockChannel = mock(Channel.class);
    when(mockConnection.createChannel()).thenReturn(mockChannel);
    client.setConnectionFactory(mockFactory);

    client.registerQueue(Queue.builder().name("testQ").build());
    client.start();

    verify(mockChannel).exchangeDeclare("testQ", "topic", true);
    verify(mockChannel).queueDeclare("testQ", true, false, false, null);
    verify(mockChannel).queueBind("testQ", "testQ", "testQ");
    verifyNoMoreInteractions(mockChannel);

    client.stop();

    verify(mockConnection).createChannel();
    verify(mockConnection).addShutdownListener(client);
    verify(mockConnection).close();
    verifyNoMoreInteractions(mockConnection);
  }

  @Test
  public void connectionFailureSchedulesReconnect() throws Exception {
    final Client client = Client.builder().addBroker(1234).addBroker(1235).build();
    final Client clientSpy = spy(client);

    final ConnectionFactory mockFactory = mock(ConnectionFactory.class);
    final Connection mockConnection = mock(Connection.class);
    when(mockFactory.newConnection())
        .thenReturn(mockConnection)
        .thenThrow(new IOException("Testing reconnection"))
        .getMock();
    doNothing().when(clientSpy).scheduleReconnect(Matchers.<URI>any());

    final Channel mockChannel = mock(Channel.class);
    when(mockConnection.createChannel()).thenReturn(mockChannel);
    clientSpy.setConnectionFactory(mockFactory);

    clientSpy.start();

    verify(clientSpy).scheduleReconnect(Matchers.<URI>any());
  }

  @Test
  public void handlingExceptionsWhenStopping() throws Exception {
    final Client client = Client.builder().addBroker(1234).addBroker(1235).build();
    final Client clientSpy = spy(client);

    final List<Connection> connections = mockedConnectionsForClient(clientSpy, 2);
    for (Connection connection : connections) {
      doThrow(IOException.class).when(connection).close();
    }
    clientSpy.start();
    clientSpy.stop();
    for (Connection connection : connections) {
      verify(connection).close();
    }
  }

  private Channel mockedChannelForClient(Client client) throws Exception {
    final ConnectionFactory mockFactory = mock(ConnectionFactory.class);

    final Connection mockConnection = mock(Connection.class);
    when(mockFactory.newConnection()).thenReturn(mockConnection);

    final Channel mockChannel = mock(Channel.class);
    when(mockChannel.basicConsume(any(String.class), any(Consumer.class)))
        .thenReturn("consumerTag");
    when(mockConnection.createChannel()).thenReturn(mockChannel);
    client.setConnectionFactory(mockFactory);
    return mockChannel;
  }

  private List<Connection> mockedConnectionsForClient(Client client, int numConnections)
      throws Exception {
    final ConnectionFactory mockFactory = mock(ConnectionFactory.class);
    final OngoingStubbing<Connection> newConnectionStub = when(mockFactory.newConnection());
    List<Connection> conns = new ArrayList<>(numConnections);
    // TODO OMG this is ugly. there's got to be a better way.
    OngoingStubbing<Connection> connectionOngoingStubbing = null;
    for (int i = 0; i < numConnections; i++) {
      final Connection mockConnection = mock(Connection.class);
      if (i == 0) {
        connectionOngoingStubbing = newConnectionStub.thenReturn(mockConnection);
      } else {
        connectionOngoingStubbing.thenReturn(mockConnection);
      }
      final Channel mockChannel = mock(Channel.class);
      when(mockConnection.createChannel()).thenReturn(mockChannel);
      conns.add(mockConnection);
    }
    client.setConnectionFactory(mockFactory);
    return conns;
  }
}
