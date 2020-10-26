package com.xing.beetle.amqp;

import com.rabbitmq.client.*;
import com.xing.beetle.dedup.spi.Deduplicator;
import com.xing.beetle.dedup.spi.KeyValueStoreBasedDeduplicator;
import com.xing.beetle.redis.RedisDedupStore;
import com.xing.beetle.util.ExceptionSupport.Supplier;
import com.xing.beetle.util.RetryExecutor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class BeetleConnectionFactory extends ConnectionFactory {

  private boolean invertRequeueParameter = true;
  private BeetleAmqpConfiguration beetleAmqpConfiguration;
  private Deduplicator deduplicator;
  private ListAddressResolver listAddressResolver;

  public BeetleConnectionFactory(
      BeetleAmqpConfiguration beetleAmqpConfiguration, Deduplicator deduplicator) {
    this.beetleAmqpConfiguration = beetleAmqpConfiguration;
    this.deduplicator = deduplicator;
    String[] addresses = beetleAmqpConfiguration.getBeetleServers().split(",");
    List<Address> parsedAddresses =
        Arrays.stream(addresses).map(Address::parseAddress).collect(Collectors.toList());
    this.listAddressResolver = new ListAddressResolver(parsedAddresses);
  }

  public BeetleConnectionFactory(BeetleAmqpConfiguration beetleAmqpConfiguration) {
    this(
        beetleAmqpConfiguration,
        new KeyValueStoreBasedDeduplicator(
            new RedisDedupStore(beetleAmqpConfiguration), beetleAmqpConfiguration));
  }

  private Supplier<RecoverableConnection> connection(
      ExecutorService executor, AddressResolver resolver, String clientProvidedName) {
    return () ->
        (RecoverableConnection) super.newConnection(executor, resolver, clientProvidedName);
  }

  @Override
  protected AddressResolver createAddressResolver(List<Address> addresses) {
    return listAddressResolver;
  }

  @Override
  public Connection newConnection(
      ExecutorService executor, AddressResolver addressResolver, String clientProvidedName)
      throws IOException {
    setAutomaticRecoveryEnabled(true);
    RetryExecutor retryExecutor = getRetryExecutor(executor);
    List<Connection> connections =
        addressResolver.getAddresses().stream()
            .map(Collections::singletonList)
            .map(ListAddressResolver::new)
            .map(res -> connection(executor, res, clientProvidedName))
            .map(retryExecutor::supply)
            .map(RetryableConnection::new)
            .map(
                c -> new RequeueAtEndConnection(c, beetleAmqpConfiguration, invertRequeueParameter))
            .map(
                delegate ->
                    new MultiPlexingConnection(delegate, deduplicator, isInvertRequeueParameter()))
            .collect(Collectors.toList());
    return new BeetleConnection(connections, beetleAmqpConfiguration);
  }

  private RetryExecutor getRetryExecutor(ExecutorService executor) {
    RetryExecutor.Builder builder = new RetryExecutor.Builder();
    if (executor != null) {
      builder.executor(executor);
    }
    return builder.build();
  }

  public boolean isInvertRequeueParameter() {
    return invertRequeueParameter;
  }

  public void setInvertRequeueParameter(boolean invertRequeueParameter) {
    this.invertRequeueParameter = invertRequeueParameter;
  }
}
