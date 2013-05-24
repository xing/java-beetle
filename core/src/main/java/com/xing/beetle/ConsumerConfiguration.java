package com.xing.beetle;

import java.util.concurrent.TimeUnit;

public class ConsumerConfiguration {
	
    private final Queue queue;
    private final MessageHandler handler;

    private int attempts = 1;

    private int exceptions = 0;

    private int retryDelay = 10;
    private TimeUnit retryDelayUnit = TimeUnit.SECONDS;

    // TODO make configurable? Nothing cancels the handler execution anyway, it's questionable if we want this...
    private int handlerTimeout = 600;
    private TimeUnit handlerTimeoutUnit = TimeUnit.SECONDS;

    public ConsumerConfiguration(Queue queue, MessageHandler handler) {
        this.queue = queue;
        this.handler = handler;
    }

	public Queue getQueue() {
		return queue;
	}

	public MessageHandler getHandler() {
		return handler;
	}

    public ConsumerConfiguration attempts(int attempts) {
        this.attempts = attempts;
        return this;
    }

    public ConsumerConfiguration exceptions(int exceptions) {
        this.exceptions = exceptions;
        return this;
    }

    public ConsumerConfiguration handlerRetryDelay(int retryDelay, TimeUnit retryDelayUnit) {
        this.retryDelay = retryDelay;
        this.retryDelayUnit = retryDelayUnit;
        return this;
    }

    public int getAttempts() {
        return attempts;
    }

    public int getExceptions() {
        return exceptions;
    }

    public int getRetryDelay() {
        return retryDelay;
    }

    public TimeUnit getRetryDelayUnit() {
        return retryDelayUnit;
    }

    public int getHandlerTimeout() {
        return handlerTimeout;
    }

    public TimeUnit getHandlerTimeoutUnit() {
        return handlerTimeoutUnit;
    }
}