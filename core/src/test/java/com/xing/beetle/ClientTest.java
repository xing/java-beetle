package com.xing.beetle;

import com.rabbitmq.client.*;
import org.junit.Assert;
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

    private Channel mockedChannelForClient(Client client) throws IOException {
        final ConnectionFactory mockFactory = mock(ConnectionFactory.class);
        final Connection mockConnection = mock(Connection.class);
        when(mockFactory.newConnection()).thenReturn(mockConnection);
        final Channel mockChannel = mock(Channel.class);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
        client.setConnectionFactory(mockFactory);
        return mockChannel;
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
    public void clientBuilderHasDefaultBroker() {
        final Client client = Client.builder().build();
        Assert.assertEquals("ClientBuilder should add at a least a default broker.", 1, client.getBrokerUris().size());
    }

    @Test
    public void clientStartStartsConsumers() throws IOException {
        final Client client = Client.builder().build();
        final Client clientSpy = spy(client);

        final Channel channel = mockedChannelForClient(clientSpy);

        Queue queue = Queue.builder().name("test-queue").build();
        clientSpy.registerQueue(queue);
        clientSpy.registerHandler(queue, new DefaultMessageHandler() {
            @Override
            public void process(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                // ignore
            }
        });
        clientSpy.start();
        verify(channel).basicConsume(eq("test-queue"), Matchers.<Consumer>anyObject());
    }
}
