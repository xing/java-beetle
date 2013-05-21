package com.xing.beetle;

import com.rabbitmq.client.*;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.*;

/**
 *
 */
public class ClientTest {

    private Channel mockedChannelForClient(Client client) throws IOException {
        final ConnectionFactory mockFactory = mock(ConnectionFactory.class);

        final Connection mockConnection = mock(Connection.class);
        when(mockFactory.newConnection()).thenReturn(mockConnection);

        final Channel mockChannel = mock(Channel.class);
        when(mockChannel.basicConsume(any(String.class), any(Consumer.class))).thenReturn("consumerTag");
        when(mockConnection.createChannel()).thenReturn(mockChannel);
        client.setConnectionFactory(mockFactory);
        return mockChannel;
    }

    private List<Connection> mockedConnectionsForClient(Client client, int numConnections) throws IOException {
        final ConnectionFactory mockFactory = mock(ConnectionFactory.class);
        final OngoingStubbing<Connection> newConnectionStub = when(mockFactory.newConnection());
        List<Connection> conns = new ArrayList<Connection>(numConnections);
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

    @Test
    public void connectDeclaresAMQPItems() throws URISyntaxException, IOException {
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
    public void connectionFailureSchedulesReconnect() throws URISyntaxException, IOException {
        final Client client = Client.builder().addBroker(1234).addBroker(1235).build();
        final Client clientSpy = spy(client);

        final ConnectionFactory mockFactory = mock(ConnectionFactory.class);
        final Connection mockConnection = mock(Connection.class);
        when(mockFactory.newConnection()).thenReturn(mockConnection).thenThrow(new IOException("Testing reconnection")).getMock();
        doNothing().when(clientSpy).scheduleReconnect(Matchers.<URI>any());

        final Channel mockChannel = mock(Channel.class);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
        clientSpy.setConnectionFactory(mockFactory);

        clientSpy.start();

        verify(clientSpy).scheduleReconnect(Matchers.<URI>any());
    }

    @Test
    public void clientStartStartsConsumers() throws IOException {
        final Client client = Client.builder().build();
        final Client clientSpy = spy(client);

        final Channel channel = mockedChannelForClient(clientSpy);

        MessageHandler handler = new MessageHandler() {
            @Override
            public Callable<HandlerResponse> doProcess(final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) {
                return new Callable<HandlerResponse>() {
                    @Override
                    public HandlerResponse call() throws Exception {
                        return HandlerResponse.ok(envelope, properties, body);
                    }
                };
            }
        };
        
        Queue queue = Queue.builder().name("test-queue").build();
        clientSpy.registerQueue(queue);
        clientSpy.registerHandler(new ConsumerConfiguration(queue, handler));
        clientSpy.start();
        verify(channel).basicConsume(eq("test-queue"), Matchers.<Consumer>anyObject());
    }

    @Test
    public void handlingExceptionsWhenStopping() throws IOException, URISyntaxException {
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
}
