package com.xing.beetle;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientBuilder {
	
    private static Logger log = LoggerFactory.getLogger(ClientBuilder.class);

	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 5672;
	private static final String DEFAULT_USERNAME = "guest";
	private static final String DEFAULT_PASSWORD = "guest";
	private static final String DEFAULT_VHOST = "/";

    private static final String DEFAULT_REDIS_HOST = "127.0.0.1";
    private static final int DEFAULT_REDIS_PORT = 6379;
	
	private List<URI> uris = new ArrayList<URI>();
    private String redisHost = DEFAULT_REDIS_HOST;
    private int redisPort = DEFAULT_REDIS_PORT;
	private ExecutorService executorService;

	public ClientBuilder addBroker(URI amqpUri) {
	    uris.add(amqpUri);
	    return this;
	}

    public ClientBuilder setRedis(String host, int port) {
        this.redisHost = host;
        this.redisPort = port;
        return this;
    }

    public ClientBuilder setRedis(String host) {
        this.redisHost = host;
        return this;
    }
	
	public ClientBuilder addBroker(String host, int port, String username, String password, String virtualHost) throws URISyntaxException {
	    return addBroker(new URI("amqp", username + ":" + password, host, port, virtualHost, null, null));
	}
	
	public ClientBuilder addBroker(String host, int port) throws URISyntaxException {
	    return addBroker(host, port, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_VHOST);
	}
	
	public ClientBuilder addBroker(String host) throws URISyntaxException {
	    return addBroker(host, DEFAULT_PORT, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_VHOST);
	}
	
	public ClientBuilder addBroker(int port) throws URISyntaxException {
	    return addBroker(DEFAULT_HOST, port, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_VHOST);
	}
	
	public ClientBuilder executorService(ExecutorService executorService) {
	    this.executorService = executorService;
	    return this;
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
	    if (executorService == null) {
	    	// We want at least 2 threads, even if we only have 2 CPUS.
	    	int nThreads = Math.max(4, Runtime.getRuntime().availableProcessors() / 2);
	        
	        log.info("Added default fixed thread pool for message handler with {} threads", nThreads);
	        final ThreadFactory messageHandlerThreadFactory = new ThreadFactory() {
	            private AtomicInteger threadNumber = new AtomicInteger(1);
	            @Override
	            public Thread newThread(Runnable r) {
	                final Thread thread = new Thread(r, "message-handler-" + threadNumber.getAndIncrement());
	                thread.setDaemon(true);
	                return thread;
	            }
	        };
	        executorService = Executors.newFixedThreadPool(nThreads, messageHandlerThreadFactory);
	    }
	    return new Client(uris, redisHost, redisPort, executorService);
	}
        
}
