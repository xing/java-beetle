package com.xing.beetle;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BeetleChannels {
	
	private static final Logger log = LoggerFactory.getLogger(BeetleChannels.class);
	
    private final Set<Channel> subscriberChannels = Collections.synchronizedSet(new HashSet<Channel>());
    private final Object publisherChannelMonitor = new Object();
    private final Connection connection;
    private Channel publisherChannel = null;

    public BeetleChannels(Connection connection) {
        this.connection = connection;
    }


    public Channel getPublisherChannel() throws IOException {
        synchronized (publisherChannelMonitor) {
            if (publisherChannel == null) {
                publisherChannel = connection.createChannel();
            }
            return publisherChannel;
        }
    }

    public Channel createSubscriberChannel() throws IOException {
        final Channel channel = connection.createChannel();
        subscriberChannels.add(channel);
        return channel;
    }

    public void removeChannel(Channel channel) {
        if (! subscriberChannels.remove(channel)) {
            // must be the publisherChannel, check that
            if (channel == publisherChannel) {
                publisherChannel = null;
            } else {
                log.error("Requested to remove unknown channel {} for connection {}", channel, connection);
            }
        }
    }

    @Override
    public String toString() {
        return "BeetleChannels{" +
            "connection=" + connection +
            ", subscriberChannels=" + subscriberChannels +
            ", publisherChannel=" + publisherChannel +
            '}';
    }
    
}
