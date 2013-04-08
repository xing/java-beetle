package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.mockito.Mockito.*;

/**
 *
 */
public class ClientTest {

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
}
