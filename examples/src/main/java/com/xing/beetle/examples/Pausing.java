package com.xing.beetle.examples;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.xing.beetle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Pausing {
	
    private Logger log = LoggerFactory.getLogger(Pausing.class);
    private AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        try {
            new Pausing().run();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void run() throws URISyntaxException, InterruptedException {

        final Client client = Client.builder()
            .addBroker(5672)
            .addBroker(5671)
            .setDeduplicationStore(new RedisConfiguration("127.0.0.1"))
            .build();

        final Queue simpleQ = Queue.builder()
            .name("simpleQ")
            .key("example.routing.key")
            .amqpName("queueNameOnBroker")
            .exchange("simpleXchg")
            .autoDelete(false)
            .build();
        client.registerQueue(simpleQ);

        final Message redundantMsg = Message.builder()
            .name("redundantMsg")
            .key("example.routing.key")
            .exchange("simpleXchg")
            .redundant(true)
            .ttl(2, TimeUnit.MINUTES)
            .build();
        client.registerMessage(redundantMsg);

        final Message nonRedundantMsg = Message.builder()
            .name("simpleMsg")
            .key("example.routing.key")
            .exchange("simpleXchg")
            .ttl(2, TimeUnit.MINUTES)
            .build();
        client.registerMessage(redundantMsg);

        final ConsumerConfiguration simpleMsgHandler = new ConsumerConfiguration(simpleQ, new MessageHandler() {
            @Override
            public Callable<HandlerResponse> doProcess(final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) {
                log.info("Received message {}", new String(body));
                return new Callable<HandlerResponse>() {
                    @Override
                    public HandlerResponse call() throws Exception {
                        log.info("Handling message...{}", "deliveryTag = " + envelope.getDeliveryTag() + " routingKey = " + envelope.getRoutingKey() + " exchange = " + envelope.getExchange());
                        // make a race condition between pausing and still use the consumer on the channel
                        Thread.sleep(500);
                        return HandlerResponse.ok(envelope, properties, body);
                    }
                };
            }
        });
        client.registerHandler(simpleMsgHandler);
        client.start();
        final CountDownLatch shutdownLatch = new CountDownLatch(2);

        Runnable publisher = new Runnable() {
            private int i = 0;
            @Override
            public void run() {
                while (running.get()) {
                    client.publish(redundantMsg, "Redundant message " + i++);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {}
                }
                shutdownLatch.countDown();
            }
        };
        Runnable subscriber = new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    try {
                        Thread.sleep(1000);
                        client.pause(simpleMsgHandler);
                        Thread.sleep(2000);
                        client.resume(simpleMsgHandler);
                    }
                    catch (InterruptedException ignored) {}
                }
                shutdownLatch.countDown();
            }
        };

        new Thread(publisher, "publisher").start();
        new Thread(subscriber, "subscriber").start();

        Thread.sleep(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
        running.set(false);
        // wait for threads to stop.
        shutdownLatch.await();

        client.stop();
    }
    
}
