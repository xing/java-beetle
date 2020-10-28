package com.xing.beetle.amqp;

import com.rabbitmq.client.*;
import com.xing.beetle.dedup.spi.Deduplicator;
import com.xing.beetle.dedup.spi.KeyValueStoreBasedDeduplicator;
import com.xing.beetle.redis.RedisDedupStore;
import com.xing.beetle.util.RetryExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
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

  private RecoverableConnection createBrokerConnection(
      ExecutorService executor, AddressResolver resolver, String clientProvidedName)
      throws IOException, TimeoutException {
    return (RecoverableConnection) super.newConnection(executor, resolver, clientProvidedName);
  }

  @Override
  protected AddressResolver createAddressResolver(List<Address> addresses) {
    return listAddressResolver;
  }

  @Override
  public Connection newConnection(
      ExecutorService executor, AddressResolver addressResolver, String clientProvidedName)
      throws IOException, TimeoutException {
    setAutomaticRecoveryEnabled(true);
    RetryExecutor retryExecutor = getRetryExecutor(executor);
    List<Connection> connections = new ArrayList<>();
    List<ListAddressResolver> addressResolvers =
        addressResolver.getAddresses().stream()
            .map(Collections::singletonList)
            .map(ListAddressResolver::new)
            .collect(Collectors.toList());

    for (ListAddressResolver resolver : addressResolvers) {
      RecoverableConnection connection =
          createBrokerConnection(executor, resolver, clientProvidedName);
      retryExecutor.supply(() -> connection);
      RequeueAtEndConnection requeueAtEndConnection =
          new RequeueAtEndConnection(
              new RetryableConnection(connection), beetleAmqpConfiguration, invertRequeueParameter);
      connections.add(
          new MultiPlexingConnection(
              requeueAtEndConnection, deduplicator, isInvertRequeueParameter()));
    }
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
