package com.xing.beetle.examples;

import com.xing.beetle.*;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class Simple {

    public static void main(String[] args) {
        try {
            new Simple().run();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void run() throws URISyntaxException {

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

        final Message simpleMsg = Message.builder()
            .name("simpleMsg")
            .key("simpleMsg")
            .exchange("simpleXchg")
            .redundant(true)
            .ttl(2, TimeUnit.MINUTES)
            .build();
        client.registerMessage(simpleMsg);

        client.registerHandler(simpleMsg, new DefaultMessageHandler() {});
        client.start();

        client.publish(simpleMsg.getName(), "some payload");

        client.stop();
    }

}
