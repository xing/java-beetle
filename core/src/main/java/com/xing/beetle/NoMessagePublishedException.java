package com.xing.beetle;

/** */
public class NoMessagePublishedException extends RuntimeException {

  private final Message beetleMessage;
  private final String payload;

  public NoMessagePublishedException(Message beetleMessage, String payload) {
    super("Message " + beetleMessage.getName() + " could not be published.");
    this.beetleMessage = beetleMessage;
    this.payload = payload;
  }

  public Message getBeetleMessage() {
    return beetleMessage;
  }

  public String getPayload() {
    return payload;
  }
}
