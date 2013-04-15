package com.xing.beetle;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Client implements ShutdownListener {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static Logger log = LoggerFactory.getLogger(Client.class);

    private LifecycleStates state;

    private final List<URI> uris;

    private ExecutorCompletionService<HandlerResponse> completionService;

    private ConnectionFactory connectionFactory;

    private final Map<Connection, URI> connections;

    private final Map<Connection, BeetleChannels> channels;

    private final Map<Future<HandlerResponse>, MessageInfo> handlerMessageInfo;

    private final ScheduledExecutorService reconnector;

    private final Set<Exchange> exchanges;

    private final Set<Queue> queues;

    private final Set<Message> messages;

    private final Set<QueueHandlerTuple> handlers;

    private AtomicBoolean running;

    protected Client(List<URI> uris, ExecutorService executorService) {
        this.uris = uris;
        connections = new ConcurrentHashMap<>(uris.size());
        channels = new ConcurrentHashMap<>();
        reconnector = new ScheduledThreadPoolExecutor(1);
        exchanges = new HashSet<Exchange>();
        queues = new HashSet<Queue>();
        messages = new HashSet<Message>();
        handlers = new HashSet<QueueHandlerTuple>();
        state = LifecycleStates.UNINITIALIZED;
        completionService = new ExecutorCompletionService<HandlerResponse>(executorService);
        running = new AtomicBoolean(false);
        handlerMessageInfo = new ConcurrentHashMap<>();
    }

    // isn't there a cleaner way of doing this in tests?
    private ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            connectionFactory = new ConnectionFactory();
        }
        return connectionFactory;
    }

    // default visibility used for injecting a mock in the tests
    void setConnectionFactory(ConnectionFactory factory) {
        connectionFactory = factory;
    }

    /**
     * Starts listening for the configured handlers.
     */
    public void start() {
        if (state == LifecycleStates.STOPPED) {
            throw new IllegalStateException("Cannot restart a stopped Beetle client. Construct a new one.");
        }
        if (state == LifecycleStates.STARTED) {
            // this call does not make sense, log a Throwable to help identifying the caller.
            log.debug("Ignoring call to start() for an already started Beetle client.", new Throwable());
            return;
        }
        running.set(true);
        for (final URI uri : uris) {
            connect(uri);
        }
        state = LifecycleStates.STARTED;
    }

    public void stop() {
        reconnector.shutdownNow();
        running.set(false);
        for (Connection connection : connections.keySet()) {
            try {
                connection.close();
            } catch (IOException e) {
                log.warn("Caught exception while closing the broker connections.", e);
            }
        }
        state = LifecycleStates.STOPPED;
    }

    protected void connect(final URI uri) {
        final ConnectionFactory factory = getConnectionFactory();
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

            final BeetleChannels beetleChannels = new BeetleChannels(connection);
            channels.put(connection, beetleChannels);
            // declare the objects using the publisher channel,
            // since at this point no one can actually use this client yet to publish anything
            final Channel channel = beetleChannels.getPublisherChannel();
            declareExchanges(channel);
            declareQueues(channel);
            declareBindings(channel);

            subscribe(beetleChannels);

        } catch (IOException e) {
            log.warn("Unable to connect to {}. Will retry in 10 seconds.", uri, e);
            scheduleReconnect(uri);
        }
        // we use only one thread for all brokers, contention should be low in this part of the code.
        new Thread(new AckNackHandler(this), "ack-nack-handler").start();
    }

    private void subscribe(final BeetleChannels beetleChannels) {
        for (QueueHandlerTuple tuple : handlers) {
            final MessageHandler handler = tuple.handler;
            final Queue queue = tuple.queue;
            log.debug("Subscribing {} to queue {}", handler, queue);
            try {
                final Channel subscriberChannel = beetleChannels.createSubscriberChannel();
                subscriberChannel.basicConsume(queue.getQueueNameOnBroker(), new DefaultConsumer(subscriberChannel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        final Callable<HandlerResponse> handlerProcessor = handler.process(envelope, properties, body);
                        try {
                            final Future<HandlerResponse> handlerResponseFuture = completionService.submit(handlerProcessor);
                            handlerMessageInfo.put(handlerResponseFuture, new MessageInfo(beetleChannels, envelope.getDeliveryTag(), envelope.getRoutingKey()));
                        } catch (RejectedExecutionException e) {
                            log.error("Could not submit message processor to executor! Requeueing message.", e);
                            subscriberChannel.basicNack(envelope.getDeliveryTag(), false, true);
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
                channels.remove(connection);
            }
        } else {
            Channel channel = (Channel)cause.getReference();
            // TODO presumably this does not mean we have an error, so we don't do anything.
            // is this supposed to be a normal application shutdown?
            log.info("AQMP channel shutdown {} because of {}. Doing nothing about it.", channel, cause.getReason());
            channels.get(channel.getConnection()).removeChannel(channel);
        }
    }

    protected void scheduleReconnect(final URI uri) {
        reconnector.schedule(new Runnable() {
            @Override
            public void run() {
                connect(uri);
            }
        }, 10, TimeUnit.SECONDS); // TODO make configurable via strategy
    }

    public static ClientBuilder builder() {
        return new ClientBuilder();
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

    public Client registerHandler(Queue queue, MessageHandler handler) {
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
     * @throws IllegalStateException if the Client object is not started or {@link #stop()} has already been called.
     * @return this Client object, to allow method chaining
     */
    public Client publish(Message message, String payload) {
        if (state == LifecycleStates.UNINITIALIZED) {
            throw new IllegalStateException("Cannot publish message, Beetle client has not be started yet.");
        }
        if (state == LifecycleStates.STOPPED) {
            throw new IllegalStateException("Cannot publish message, Beetle client has already been stopped.");
        }

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
            final BeetleChannels beetleChannels = channels.get(connection);

            try {
                // this channel should be open already, but if it isn't this will open a new one.
                final Channel channel = beetleChannels.getPublisherChannel();
                channel.basicPublish(
                    message.getExchange().getName(),
                    message.getKey(),
                    null, // TODO publish settings
                    payload
                );
                successfulSends++;
            } catch (IOException e) {
                log.warn("Unable to publish message {} to broker {}. Trying next broker.", message, connections.get(connection));
            }
        }
        return successfulSends;
    }

    public Set<URI> getBrokerUris() {
        return new HashSet<URI>(uris);
    }
    
    public boolean isRunning() {
    	return running.get();
    }
    
    public MessageInfo takeMessageInfo(Future<HandlerResponse> handlerResponseFuture) {
    	return handlerMessageInfo.remove(handlerResponseFuture);
    }
    
    public Future<HandlerResponse> pollForHandlerResponse() throws InterruptedException {
    	return completionService.poll(500, TimeUnit.MILLISECONDS);
    }

    private static class QueueHandlerTuple {
        public final Queue queue;
        public final MessageHandler handler;

        public QueueHandlerTuple(Queue queue, MessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }
    }

}
