package com.xing.beetle;

public class ConsumerConfiguration {
	
    private final Queue queue;
    private final MessageHandler handler;

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
	
}