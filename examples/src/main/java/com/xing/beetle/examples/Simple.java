package com.xing.beetle.examples;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.xing.beetle.Client;
import com.xing.beetle.DefaultMessageHandler;
import com.xing.beetle.Message;
import com.xing.beetle.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class Simple {

    private Logger log = LoggerFactory.getLogger(Simple.class);

    public static void main(String[] args) {
        try {
            new Simple().run();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void run() throws URISyntaxException, InterruptedException {

        Client client = Client.builder()
            .addBroker(5672)
            .addBroker(5671)
            .build();

        // these are the default settings, except of course the name() option
        // You can also pass the builder instead of the built object, if you plan to reuse the builder.
//        final Exchange simpleXchg = Exchange.builder().name("simpleXchg").topic(true).durable(true).build();
//        client.registerExchange(simpleXchg);

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

        client.registerHandler(simpleQ, new DefaultMessageHandler() {
            @Override
            public void process(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                log.warn("Received message {}", new String(body));
            }
        });
        client.start();

        client.publish(redundantMsg, "some payload");

        client.publish(nonRedundantMsg, "some other payload");

        System.err.println("sleeping for good measure (to actually receive the messages)");
        Thread.sleep(10 * 1000);
        client.stop();
    }

}
