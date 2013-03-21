package com.xing.beetle;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Client implements ShutdownListener {

    private static Logger log = LoggerFactory.getLogger(Client.class);

    private final List<URI> uris;

    private final HashMap<Connection, URI> connections;

    private final ScheduledExecutorService reconnector;

    public Client(List<URI> uris) {
        this.uris = uris;
        connections = new HashMap<Connection, URI>(uris.size());
        reconnector = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * Starts listening for the configured handlers.
     */
    public void start() {
        for (final URI uri : uris) {
            connect(uri);
        }
    }

    public void stop() {
        reconnector.shutdownNow();
        for (Connection connection : connections.keySet()) {
            try {
                connection.close();
            } catch (IOException e) {
                log.warn("Caught exception while closing the broker connections.", e);
            }
        }
    }

    protected void connect(final URI uri) {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(uri.getHost());
        factory.setPort(uri.getPort());
        final String[] userPass = uri.getUserInfo().split(":");
        if (userPass.length != 2) {
            log.warn("Dubious user/password information in the broker URI {}, userInfo {}. Falling back to RabbitMQ defaults.", uri, uri.getUserInfo());
        } else {
            factory.setUsername(userPass[0]);
            factory.setPassword(userPass[1]);
        }
        factory.setVirtualHost(uri.getPath());

        try {
            final Connection connection = factory.newConnection();
            connection.addShutdownListener(this);
            connections.put(connection, uri);
            log.info("Successfully connected to broker at {}", connection);
        } catch (IOException e) {
            log.warn("Unable to connect to {}. Will retry in 10 seconds.", uri, e);
            scheduleReconnect(uri);
        }
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        if (cause.isHardError()) {
            // type-cast hippies...
            Connection connection = (Connection)cause.getReference();
            if (!cause.isInitiatedByApplication()) {
                // this presumably is a error with the broker, let's reconnect
                log.warn("Connection to the broker at {}:{} lost, reconnecting in 10 seconds. Reason: {}",
                    connection.getAddress(), connection.getPort(), cause.getReason());
                final URI uri = connections.remove(connection);
                if (uri != null) {
                    scheduleReconnect(uri);
                }
            } else {
                // clean shutdown, don't reconnect automatically
                log.debug("Connection {} closed because of {}", connection, cause.getReason());
            }
        } else {
            Channel channel = (Channel)cause.getReference();
            // TODO presumably this does not mean we have an error, so we don't do anything.
            // is this supposed to be a normal application shutdown?
            log.info("AQMP channel shutdown {} because of {}. Doing nothing about it.", channel, cause.getReason());
        }
    }

    private void scheduleReconnect(final URI uri) {
        reconnector.schedule(new Runnable() {
            @Override
            public void run() {
                connect(uri);
            }
        }, 10, TimeUnit.SECONDS);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Client registerExchange(Exchange exchange) {
        return this;
    }

    public Client registerExchange(Exchange.Builder builder) {
        return registerExchange(builder.build());
    }

    public Client registerQueue(Queue queue) {
        return this;
    }

    public Client registerQueue(Queue.Builder builder) {
        return registerQueue(builder.build());
    }

    public Client registerMessage(Message message) {
        return this;
    }

    public Client registerMessage(Message.Builder builder) {
        return registerMessage(builder.build());
    }

    public Client registerHandler(String messageName, DefaultMessageHandler handler) {
        return this;
    }

    public Client publish(String simpleMsg, String payload) {
        return this;
    }

    public static class Builder {

        private static final String DEFAULT_HOST = "localhost";
        private static final int DEFAULT_PORT = 5672;
        private static final String DEFAULT_USERNAME = "guest";
        private static final String DEFAULT_PASSWORD = "guest";
        private static final String DEFAULT_VHOST = "/";

        private List<URI> uris = new ArrayList<URI>();

        public Builder addBroker(URI amqpUri) throws URISyntaxException {
            uris.add(amqpUri);
            return this;
        }

        public Builder addBroker(String host, int port, String username, String password, String virtualHost) throws URISyntaxException {
            return addBroker(new URI("amqp", username + ":" + password, host, port, virtualHost, null, null));
        }

        public Builder addBroker(String host, int port) throws URISyntaxException {
            return addBroker(host, port, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_VHOST);
        }

        public Builder addBroker(String host) throws URISyntaxException {
            return addBroker(host, DEFAULT_PORT, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_VHOST);
        }

        public Builder addBroker(int port) throws URISyntaxException {
            return addBroker(DEFAULT_HOST, port, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_VHOST);
        }

        public Client build() {
            // add at least one uri
            if (uris.size() == 0) {
                try {
                    addBroker(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_VHOST);
                } catch (URISyntaxException e) {
                    // ignore
                }
            }
            return new Client(uris);
        }
    }
}
