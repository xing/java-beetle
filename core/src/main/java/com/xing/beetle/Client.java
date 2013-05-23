package com.xing.beetle;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;

public class Client implements ShutdownListener {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private LifecycleStates state;

    private final List<URI> uris;

    private ConnectionFactory connectionFactory;

    private final Map<Connection, URI> connections;

    private final Map<Connection, BeetleChannels> channels;

    private final ScheduledExecutorService reconnector;

    private final Set<Exchange> exchanges;

    private final Set<Queue> queues;

    private final Map<String, Message> messages;

    private final Set<ConsumerConfiguration> handlers;

    private final DeduplicationStore deduplicationStore;
    private final RedisConfiguration deduplicationStoreConfig;

    private final RedisFailoverManager redisFailoverManager;

    private final ScheduledExecutorService redisFailoverExecutor;
    private final ExecutorService handlerExecutor;

    protected Client(List<URI> uris, RedisConfiguration dedupConfig, String redisFailOverMasterFile, ExecutorService executorService) {
        this.uris = uris;
        connections = new ConcurrentHashMap<>(uris.size());
        channels = new ConcurrentHashMap<>();
        reconnector = new ScheduledThreadPoolExecutor(1);
        exchanges = new HashSet<>();
        queues = new HashSet<>();
        messages = new HashMap<>();
        handlers = new HashSet<>();
        state = LifecycleStates.UNINITIALIZED;
        handlerExecutor = executorService;
        redisFailoverExecutor = Executors.newSingleThreadScheduledExecutor();
        redisFailoverManager = new RedisFailoverManager(this, redisFailOverMasterFile, dedupConfig);
        deduplicationStoreConfig = dedupConfig;
        deduplicationStore = new DeduplicationStore(dedupConfig);
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

        // Watcher for redis master file. An external daemon is updating this file in case of a redis master switch.
        if (redisFailoverManager.hasMasterFile()) {
            redisFailoverExecutor.scheduleAtFixedRate(redisFailoverManager, 0, 1, TimeUnit.SECONDS);
        } else {
            log.info("Not starting Redis failover manager because no master file path was set.");
        }

        for (final URI uri : uris) {
            connect(uri);
        }

        state = LifecycleStates.STARTED;
    }

    public void stop() {
        if (state == LifecycleStates.STOPPED) {
            log.debug("Ignoring call to stop() for an already stopped Beetle client.", new Throwable());
            return;
        }
        reconnector.shutdownNow();
        // first stop the consumers by shutting down the amqp connections
        for (Connection connection : connections.keySet()) {
            try {
                connection.close();
            } catch (IOException e) {
                log.warn("Caught exception while closing the broker connections.", e);
            }
        }

        redisFailoverExecutor.shutdownNow();

        // and _after_ that close the redis connections, because handlers might still use them
        deduplicationStore.close();
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
    }

    private void subscribe(final BeetleChannels beetleChannels) {
        for (ConsumerConfiguration tuple : handlers) {
            subscribe(beetleChannels, tuple);
        }
    }

    private void subscribe(final BeetleChannels beetleChannels, ConsumerConfiguration config) {
        final MessageHandler handler = config.getHandler();
        final Queue queue = config.getQueue();
        log.debug("Subscribing {} to queue {}", handler, queue);
        try {
            final Channel subscriberChannel = beetleChannels.createSubscriberChannel();
            final String consumerTag = subscriberChannel.basicConsume(queue.getQueueNameOnBroker(), new BeetleConsumer(this, subscriberChannel, handler));
            // potential race condition between setting the consumer tag and pausing the subscriber. Highly unlikely, though.
            handler.addChannel(subscriberChannel, consumerTag);
        } catch (IOException e) {
            log.error("Cannot subscribe {} to queue {}: {}", handler, queue, e.getMessage());
        }
    }

    public void pause(ConsumerConfiguration config) {
        config.getHandler().pause();
        handlers.remove(config);
    }

    public void resume(ConsumerConfiguration config) {
        log.info("Resuming subscriber for {}", config.getQueue());
        if (!handlers.contains(config)) {
            handlers.add(config);

            for (BeetleChannels beetleChannels : channels.values()) {
                subscribe(beetleChannels, config);
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
            Connection connection = (Connection) cause.getReference();
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
            Channel channel = (Channel) cause.getReference();
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
        messages.put(message.getName(), message);
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

    public Client registerHandler(ConsumerConfiguration config) {
        if (!queues.contains(config.getQueue())) {
            throw new IllegalArgumentException("Message " + config.getQueue() + " must be pre-declared.");
        }
        handlers.add(config);
        return this;
    }

    public Client publish(String messageName, String payload) {
        final Message message = messages.get(messageName);
        if (message == null) {
            throw new IllegalArgumentException("Trying to publish an undeclared message named " + messageName);
        }
        publish(message, payload);
        return this;
    }

    /**
     * Publish a message with the given payload.
     * <p/>
     * Depending on the message configuration it will be sent redundantly to two brokers, or use the failover handling
     * to send it to one connected broker.
     * <p/>
     * If the message could not be sent to at least one broker, an unchecked {@link NoMessagePublishedException} is thrown.
     *
     * @param message a configured {@link Message Beetle message} to publish the payload under
     * @param payload an arbitrary string value to publish (assumed to be encoded in UTF-8)
     * @return this Client object, to allow method chaining
     * @throws IllegalStateException if the Client object is not started or {@link #stop()} has already been called.
     */
    public Client publish(Message message, String payload) {
        if (state == LifecycleStates.UNINITIALIZED) {
            throw new IllegalStateException("Cannot publish message, Beetle client has not be started yet.");
        }
        if (state == LifecycleStates.STOPPED) {
            throw new IllegalStateException("Cannot publish message, Beetle client has already been stopped.");
        }

        log.info("Publishing `{}` for {}", payload, message);
        int successfulSends;
        if (message.isRedundant()) {
            // publish to at least two brokers
            successfulSends = doPublish(message, payload.getBytes(UTF8), 2);
            switch (successfulSends) {
                case 0:
                    log.error("No message published.");
                    throw new NoMessagePublishedException(message, payload);
                case 1:
                    log.warn("{} (payload: {}) was not published redundantly.", message, payload);
                    break;
                case 2:
                    // ignore, all fine
                    log.debug("{} successfully published.", message);
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
                    log.debug("{} successfully published.", message);
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

        final HashMap<String, Object> headers = new HashMap<>();
        final String messageId = UUID.randomUUID().toString();
        headers.put("format_version", "1");
        headers.put("flags", message.isRedundant() ? "1" : "0");
        final long ttl = TimeUnit.SECONDS.convert(message.getDuration(), message.getTimeUnit());
        headers.put("expires_at", Long.toString((System.currentTimeMillis() / 1000L) + ttl));

        while (successfulSends < requiredSends && connectionIterator.hasNext()) {
            final Connection connection = connectionIterator.next();
            final BeetleChannels beetleChannels = channels.get(connection);

            try {
                // this channel should be open already, but if it isn't this will open a new one.
                final Channel channel = beetleChannels.getPublisherChannel();

                final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .messageId(messageId)
                    .headers(headers)
                    .build();

                channel.basicPublish(
                    message.getExchange().getName(),
                    message.getKey(),
                    properties,
                    payload
                );
                log.debug("Published message id {} to broker {}", messageId, channel.getConnection().getAddress());

                successfulSends++;
            } catch (IOException e) {
                log.warn("Unable to publish {} to broker {}. Trying next broker.", message, connections.get(connection));
            }
        }
        return successfulSends;
    }

    public Set<URI> getBrokerUris() {
        return new HashSet<>(uris);
    }

    public RedisConfiguration getDeduplicationStoreConfiguration() {
        return deduplicationStoreConfig;
    }

    public Future<?> submit(Runnable handlerTask) {
        return handlerExecutor.submit(handlerTask);
    }

    public boolean shouldProcessMessage(Channel channel, long deliveryTag, String messageId) {

        try {
            if (deduplicationStore.isMessageNew(messageId)) {
                deduplicationStore.incrementAttempts(messageId);
                return true;
            }

            final HandlerStatus handlerStatus = deduplicationStore.getHandlerStatus(messageId);

            if (handlerStatus.isCompleted()) {
                log.debug("Handler complete for {}", messageId);
                try {
                    channel.basicAck(deliveryTag, false);
                } catch (IOException e) {
                    log.error("Could not ACK message " + messageId, e);
                }
                // no need to handle anything, this message was already handled by some other consumer
                return false;
            }

            if (handlerStatus.shouldDelay()) {
                log.debug("Handler should delay message {}", messageId);
                // processing message should still be delayed, requeue
                rejectMessage(channel, deliveryTag, messageId, true, 250);
                return false;
            }

            if (! handlerStatus.isTimedOut()) {
                // another handler is working on this message, we need to reprocess this message later
                // to determine whether we have to re-execute the handler
                log.debug("Handler timed out for message {}", messageId);
                rejectMessage(channel, deliveryTag, messageId, true, 250);
                return false;
            }

            final long attempts = handlerStatus.getAttempts();
            final long exceptions = handlerStatus.getExceptions();
            if (attempts > 1 || exceptions > 1) {
                // message handler has been tried too many times or produced too many exceptions
                log.debug("Attempt ({}) or exception ({}) count exceeded", attempts, exceptions);
                rejectMessage(channel, deliveryTag, messageId, false, 0);
                return false;
            }

            if (! acquireSharedHandlerMutex(messageId)) {
                // we could not acquire the mutex, so we need to requeue the message and check for execution later.
                log.debug("Could not acquire mutex for message {}, requeueing to process again later.", messageId);
                rejectMessage(channel, deliveryTag, messageId, true, 0);
                return false;
            }
        } catch (IllegalStateException ise) {
            log.warn("Could not process message, no redis available. Requeueing message " + messageId, ise);
            return false;
        } catch (AlreadyClosedException ace) {
            return false;
        }
        // mutex acquired, no one else is working on this message, so we handle it
        return true;
    }

    private void rejectMessage(Channel channel, long deliveryTag, String messageId, boolean requeue, long delay) {
        try {
            if (delay > 0) {
                try {
                    Thread.sleep(delay); // delay execution to prevent very fast re-delivery of this message
                } catch (InterruptedException ignore) {}
            }
            channel.basicNack(deliveryTag, false, requeue);
        } catch (IOException e) {
            log.error("Could not NACK message " + messageId, e);
        }
    }

    public void markMessageAsCompleted(String messageId) {
        deduplicationStore.markMessageCompleted(messageId);
    }

    public long incrementExceptions(String messageId) {
        return deduplicationStore.incrementExceptions(messageId);
    }

    public void removeMessageHandlerLock(String messageId) {
        deduplicationStore.removeMessageHandlerLock(messageId);
    }

    public boolean acquireSharedHandlerMutex(String messageId) {
        return deduplicationStore.acquireSharedHandlerMutex(messageId);
    }

    public long getAttemptsCount(String messageId) {
        return deduplicationStore.getAttempts(messageId);
    }

    public String getCurrentRedisMaster() {
        return redisFailoverManager.getCurrentMaster();
    }

    public DeduplicationStore getDeduplicationStore() {
        return deduplicationStore;
    }
}
