package com.xing.beetle;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Client implements ShutdownListener {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static class QueueHandlerTuple {
        public final Queue queue;
        public final DefaultMessageHandler handler;

        public QueueHandlerTuple(Queue queue, DefaultMessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }
    }

    private static Logger log = LoggerFactory.getLogger(Client.class);

    private final List<URI> uris;

    private final Map<Connection, URI> connections; // TODO synchronize access

    private final ScheduledExecutorService reconnector;

    private final Set<Exchange> exchanges;

    private final Set<Queue> queues;

    private final Set<Message> messages;

    private final Set<QueueHandlerTuple> handlers;

    public Client(List<URI> uris) {
        this.uris = uris;
        connections = new HashMap<Connection, URI>(uris.size());
        reconnector = new ScheduledThreadPoolExecutor(1);
        exchanges = new HashSet<Exchange>();
        queues = new HashSet<Queue>();
        messages = new HashSet<Message>();
        handlers = new HashSet<QueueHandlerTuple>();
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
            log.info("Successfully connected to broker at {}", connection);

            final Channel channel = connection.createChannel();
            declareExchanges(channel);
            declareQueues(channel);
            declareBindings(channel);
            subscribe(channel);

            connection.addShutdownListener(this);
            connections.put(connection, uri);

        } catch (IOException e) {
            log.warn("Unable to connect to {}. Will retry in 10 seconds.", uri, e);
            scheduleReconnect(uri);
        }
    }

    private void subscribe(final Channel channel) {
        for (QueueHandlerTuple tuple : handlers) {
            final DefaultMessageHandler handler = tuple.handler;
            final Queue queue = tuple.queue;
            log.debug("Subscribing {} to queue {}", handler, queue);
            try {
                channel.basicConsume(queue.getQueueNameOnBroker(), new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        try {
                            handler.process(envelope, properties, body);
                            log.debug("Sending ACK for successfully handled message (routing key {})", envelope.getRoutingKey());
                            channel.basicAck(envelope.getDeliveryTag(), false);
                        } catch (Exception e) {
                            // NACK the message if the handler threw an exception
                            log.error("Handler threw an exception, send NACK for message", e);
                            channel.basicNack(envelope.getDeliveryTag(), false, true);
                        }
                    }
                });
            } catch (IOException e) {
                log.error("Cannot subscribe {} to queue {}: {}", handler, queue, e.getMessage());
            }
        }
    }

    private void declareQueues(Channel channel) throws IOException {
        for (Queue queue : queues) {
            channel.queueDeclare(queue.getQueueNameOnBroker(), true, false, queue.isAutoDelete(), null);
            log.debug("Declared queue {}", queue);
        }
    }

    private void declareExchanges(Channel channel) throws IOException {
        for (Exchange exchange : exchanges) {
            channel.exchangeDeclare(exchange.getName(), exchange.isTopic() ? "topic" : "direct", exchange.isDurable());
            log.debug("Declared exchange {}", exchange);
        }
    }

    private void declareBindings(Channel channel) throws IOException {
        for (Queue queue : queues) {
            channel.queueBind(queue.getQueueNameOnBroker(), queue.getExchangeName(), queue.getKey());
            log.debug("Bound queue {}/{} to exchange {} using routing key {}",
                queue.getName(), queue.getQueueNameOnBroker(), queue.getExchange(), queue.getKey());
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
                connections.remove(connection);
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
        exchanges.add(exchange);
        return this;
    }

    public Client registerExchange(Exchange.Builder builder) {
        return registerExchange(builder.build());
    }

    public Client registerQueue(Queue queue) {
        queues.add(queue);
        ensureExchange(queue.getExchange());
        return this;
    }

    public Client registerQueue(Queue.Builder builder) {
        return registerQueue(builder.build());
    }

    public Client registerMessage(Message message) {
        messages.add(message);
        ensureExchange(message.getExchange());
        return this;
    }

    public Client registerMessage(Message.Builder builder) {
        return registerMessage(builder.build());
    }

    private void ensureExchange(Exchange exchange) {
        // register all exchanges that haven't been registered manually yet.
        if (!exchanges.contains(exchange)) {
            log.info("Auto-registering exchange {} because it wasn't registered manually. Check that you didn't want to register it manually.", exchange);
            registerExchange(exchange);
        }
    }

    public Client registerHandler(Queue queue, DefaultMessageHandler handler) {
        if (!queues.contains(queue)) {
            throw new IllegalArgumentException("Message " + queue + " must be pre-declared.");
        }
        handlers.add(new QueueHandlerTuple(queue, handler));
        return this;
    }

    /**
     * Publish a message with the given payload.
     *
     * Depending on the message configuration it will be sent redundantly to two brokers, or use the failover handling
     * to send it to one connected broker.
     *
     * If the message could not be sent to at least one broker, an unchecked {@link NoMessagePublishedException} is thrown.
     *
     * @param message a configured {@link Message Beetle message} to publish the payload under
     * @param payload an arbitrary string value to publish (assumed to be encoded in UTF-8)
     * @return this Client object, to allow method chaining
     */
    public Client publish(Message message, String payload) {
        log.debug("Publishing `{}` for message {}", payload, message);
        int successfulSends;
        if (message.isRedundant()) {
            // publish to at least two brokers
            successfulSends = doPublish(message, payload.getBytes(UTF8), 2);
            switch (successfulSends) {
                case 0:
                    log.error("No message published.");
                    throw new NoMessagePublishedException(message, payload);
                case 1:
                    log.warn("Message {} (payload: {}) was not published redundantly.", message, payload);
                    break;
                case 2:
                    // ignore, all fine
                    log.debug("Message {} successfully published.", message);
                    break;
                default:
                    log.error("Unexpected number of published messages for a redundant message. Should be 2 but {} copies published.", successfulSends);
            }
        } else {
            // publish to one broker, failing over to the next one, until it's delivered
            successfulSends = doPublish(message, payload.getBytes(UTF8), 1);
            switch (successfulSends) {
                case 0:
                    log.error("No message published.");
                    throw new NoMessagePublishedException(message, payload);
                case 1:
                    // ignore, all fine
                    log.debug("Message {} successfully published.", message);
                    break;
                default:
                    log.error("Unexpected number of published messages for a non-redundant message. Should be 1 but {} copies published.", successfulSends);
            }
        }

        return this;
    }

    private int doPublish(Message message, byte[] payload, int requiredSends) {
        int successfulSends = 0;
        final Iterator<Connection> connectionIterator = connections.keySet().iterator();
        while (successfulSends < requiredSends && connectionIterator.hasNext()) {
            final Connection connection = connectionIterator.next();
            Channel channel = null;
            try {
                // TODO re-use channel
                channel = connection.createChannel();
                channel.basicPublish(
                    message.getExchange().getName(),
                    message.getKey(),
                    null, // TODO publish settings
                    payload
                );
                successfulSends++;
            } catch (IOException e) {
                log.warn("Unable to publish message {} to broker {}. Trying next broker.", message, connections.get(connection));
            } finally {
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (IOException e) {
                        log.debug("Unable to close channel {}. Ignoring.", channel);
                        // ignore
                    }
                }
            }
        }
        return successfulSends;
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
                    log.info("Added default URI for local broker, because none was configured.");
                } catch (URISyntaxException e) {
                    // ignore
                }
            }
            return new Client(uris);
        }
    }
}
