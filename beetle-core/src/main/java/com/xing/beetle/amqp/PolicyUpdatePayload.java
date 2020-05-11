package com.xing.beetle.amqp;

class PolicyUpdatePayload {
  private String server;
  private String queue_name;
  private String dead_letter_queue_name;
  private boolean dead_lettering;
  private boolean lazy;
  private int message_ttl;
  private Bindings bindings;

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public String getQueue_name() {
    return queue_name;
  }

  public void setQueue_name(String queue_name) {
    this.queue_name = queue_name;
  }

  public String getDead_letter_queue_name() {
    return dead_letter_queue_name;
  }

  public void setDead_letter_queue_name(String dead_letter_queue_name) {
    this.dead_letter_queue_name = dead_letter_queue_name;
  }

  public boolean isDead_lettering() {
    return dead_lettering;
  }

  public void setDead_lettering(boolean dead_lettering) {
    this.dead_lettering = dead_lettering;
  }

  public boolean isLazy() {
    return lazy;
  }

  public void setLazy(boolean lazy) {
    this.lazy = lazy;
  }

  public int getMessage_ttl() {
    return message_ttl;
  }

  public void setMessage_ttl(int message_ttl) {
    this.message_ttl = message_ttl;
  }

  public Bindings getBindings() {
    return bindings;
  }

  public void setBindings(Bindings bindings) {
    this.bindings = bindings;
  }

  class Bindings {
    private String exchange;
    private String key;

    public String getExchange() {
      return exchange;
    }

    public void setExchange(String exchange) {
      this.exchange = exchange;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }
  }
}
