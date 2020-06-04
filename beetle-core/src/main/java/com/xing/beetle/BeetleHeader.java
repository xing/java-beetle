package com.xing.beetle;

public interface BeetleHeader {
  String PUBLISH_REDUNDANCY = "x-publish-message-redundancy";
  // unix timestamp after which the message should be considered stale (seconds)
  String EXPIRES_AT = "expires_at";
}
