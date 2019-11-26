package com.xing.beetle;

public interface BeetleHeader {

  String PUBLISH_REDUNDANCY = "x-publish-message-redundancy";
  /** Either a long in milliseconds, or a String, which is parsed as Duration.parse() */
  String REQUEUE_AT_END_DELAY = "x-requeue-rejected-messages-at-end-delay";
}
