package com.xing.beetle;

public class RedisConfiguration {

  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final int DEFAULT_PORT = 6379;

  private final String host;
  private final int port;

  public RedisConfiguration() {
    this.host = DEFAULT_HOST;
    this.port = DEFAULT_PORT;
  }

  public RedisConfiguration(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public RedisConfiguration(String host) {
    this.host = host;
    this.port = DEFAULT_PORT;
  }

  public String getHostname() {
    return host;
  }

  public int getPort() {
    return port;
  }
}
