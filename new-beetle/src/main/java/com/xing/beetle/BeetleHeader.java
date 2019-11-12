package com.xing.beetle;

public interface BeetleHeader {

  String PUBLISH_REDUNDANCY = "x-publish-message-redundancy";
  String REQUEUE_AT_END_DELAY = "x-requeue-rejected-messages-at-end-delay";
}
