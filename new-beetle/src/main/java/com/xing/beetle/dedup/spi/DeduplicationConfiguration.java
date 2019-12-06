package com.xing.beetle.dedup.spi;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "beetle.dedup")
public class DeduplicationConfiguration {
  // default values
  private int handlerTimeout = 600;
  private int maxHandlerExecutionAttempts = 1;
  private int handlerExecutionAttemptsDelay = 10;
  private int exceptionLimit = 0;

  int getHandlerTimeout() {
    return handlerTimeout;
  }

  public void setHandlerTimeout(int handlerTimeout) {
    this.handlerTimeout = handlerTimeout;
  }

  int getMaxHandlerExecutionAttempts() {
    if (maxHandlerExecutionAttempts <= exceptionLimit) {
      return exceptionLimit + 1;
    }
    return maxHandlerExecutionAttempts;
  }

  public void setMaxHandlerExecutionAttempts(int maxHandlerExecutionAttempts) {
    this.maxHandlerExecutionAttempts = maxHandlerExecutionAttempts;
  }

  int getHandlerExecutionAttemptsDelay() {
    return handlerExecutionAttemptsDelay;
  }

  public void setHandlerExecutionAttemptsDelay(int handlerExecutionAttemptsDelay) {
    this.handlerExecutionAttemptsDelay = handlerExecutionAttemptsDelay;
  }

  int getExceptionLimit() {
    return exceptionLimit;
  }

  public void setExceptionLimit(int exceptionLimit) {
    this.exceptionLimit = exceptionLimit;
  }

  int getMutexExpiration() {
    return 2 * handlerTimeout;
  }

  int getMaxhandlerExecutionAttemptsDelay() {
    return 2 * handlerExecutionAttemptsDelay;
  }
}
