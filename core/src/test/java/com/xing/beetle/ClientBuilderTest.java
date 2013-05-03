package com.xing.beetle;

import static org.junit.Assert.*;
import org.junit.Test;

import java.net.URI;

public class ClientBuilderTest {

    @Test
    public void testSetsDeduplicationStore() throws Exception {
        // Using 0.0.0.0 because 127.0.0.1 is the automatic default and we want to test it is actually being set.
        RedisConfiguration redis = new RedisConfiguration("0.0.0.0");
        Client client = Client.builder().setDeduplicationStore(redis).build();
        assertEquals("0.0.0.0", client.getDeduplicationStoreConfiguration().getHostname());
    }

    @Test
    public void testAddBrokerWorksWithDefault() throws Exception {
        Client client = Client.builder().build();
        assertEquals("amqp://guest:guest@localhost:5672/", client.getBrokerUris().toArray(new URI[1])[0].toString());
    }

    @Test
    public void testAddBrokerWorksWithAmqpUri() throws Exception {
        URI uri = new URI("amqp", "somebody:pass", "example.org", 1234, "/", null, null);
        Client client = Client.builder().addBroker(uri).build();
        assertEquals("amqp://somebody:pass@example.org:1234/", client.getBrokerUris().toArray(new URI[1])[0].toString());
    }

    @Test
    public void testAddBrokerWorksFullyDefined() throws Exception {
        Client client = Client.builder().addBroker("0.0.0.0", 5555, "foo", "bar", "/baz").build();
        assertEquals("amqp://foo:bar@0.0.0.0:5555/baz", client.getBrokerUris().toArray(new URI[1])[0].toString());
    }

    @Test
    public void testAddBrokerWorksWithHostAndPort() throws Exception {
        Client client = Client.builder().addBroker("example.org", 5555).build();
        assertEquals("amqp://guest:guest@example.org:5555/", client.getBrokerUris().toArray(new URI[1])[0].toString());
    }

    @Test
    public void testAddBrokerWorksWithPort() throws Exception {
        Client client = Client.builder().addBroker(1234).build();
        assertEquals("amqp://guest:guest@localhost:1234/", client.getBrokerUris().toArray(new URI[1])[0].toString());
    }

    @Test
    public void testAddBrokerWorksWithVirtualHostNotStartingWithSlash() throws Exception {
        Client client = Client.builder().addBroker("0.0.0.0", 5555, "foo", "bar", "noslash").build();
        assertEquals("amqp://foo:bar@0.0.0.0:5555/noslash", client.getBrokerUris().toArray(new URI[1])[0].toString());
    }
}
