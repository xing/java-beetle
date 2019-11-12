package com.xing.beetle;

public enum LifecycleState {
  UNINITIALIZED,
  STARTING,
  STARTED,
  STOPPING,
  STOPPED;

  public boolean isRunning() {
    return this == STARTED;
  }
}
