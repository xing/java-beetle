package com.xing.beetle.spring;

import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.dedup.spi.Deduplicator;
import com.xing.beetle.dedup.spi.DeduplicationConfiguration;
import com.xing.beetle.dedup.spi.KeyValueStore;
import com.xing.beetle.dedup.spi.KeyValueStoreBasedDeduplicator;
import com.xing.beetle.spring.BeetleAutoConfiguration.BeetleConnectionFactoryCreator;
import org.aopalliance.aop.Advice;
import org.springframework.amqp.rabbit.config.AbstractRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.amqp.DirectRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.Arrays;

@Configuration
@Import(BeetleConnectionFactoryCreator.class)
class BeetleAutoConfiguration {

  @Configuration
  static class BeetleConnectionFactoryCreator {

    @Bean
    @ConditionalOnMissingBean
    BeetleConnectionFactory beetleConnectionFactory() {
      BeetleConnectionFactory factory = new BeetleConnectionFactory();
      factory.setInvertRequeueParameter(true);
      return factory;
    }

    private RabbitConnectionFactoryBean getRabbitConnectionFactoryBean(RabbitProperties properties)
        throws Exception {
      PropertyMapper map = PropertyMapper.get();
      RabbitConnectionFactoryBean factory =
          new CustomizableConnectionFactoryBean(beetleConnectionFactory());
      map.from(properties::determineHost).whenNonNull().to(factory::setHost);
      map.from(properties::determinePort).to(factory::setPort);
      map.from(properties::determineUsername).whenNonNull().to(factory::setUsername);
      map.from(properties::determinePassword).whenNonNull().to(factory::setPassword);
      map.from(properties::determineVirtualHost).whenNonNull().to(factory::setVirtualHost);
      map.from(properties::getRequestedHeartbeat)
          .whenNonNull()
          .asInt(Duration::getSeconds)
          .to(factory::setRequestedHeartbeat);
      RabbitProperties.Ssl ssl = properties.getSsl();
      if (ssl.isEnabled()) {
        factory.setUseSSL(true);
        map.from(ssl::getAlgorithm).whenNonNull().to(factory::setSslAlgorithm);
        map.from(ssl::getKeyStoreType).to(factory::setKeyStoreType);
        map.from(ssl::getKeyStore).to(factory::setKeyStore);
        map.from(ssl::getKeyStorePassword).to(factory::setKeyStorePassphrase);
        map.from(ssl::getTrustStoreType).to(factory::setTrustStoreType);
        map.from(ssl::getTrustStore).to(factory::setTrustStore);
        map.from(ssl::getTrustStorePassword).to(factory::setTrustStorePassphrase);
        map.from(ssl::isValidateServerCertificate)
            .to((validate) -> factory.setSkipServerCertificateValidation(!validate));
        map.from(ssl::getVerifyHostname).to(factory::setEnableHostnameVerification);
      }
      map.from(properties::getConnectionTimeout)
          .whenNonNull()
          .asInt(Duration::toMillis)
          .to(factory::setConnectionTimeout);
      factory.afterPropertiesSet();
      return factory;
    }

    @Bean
    ConnectionFactory rabbitConnectionFactory(
        RabbitProperties properties, ObjectProvider<ConnectionNameStrategy> connectionNameStrategy)
        throws Exception {
      PropertyMapper map = PropertyMapper.get();
      CachingConnectionFactory factory =
          new CachingConnectionFactory(getRabbitConnectionFactoryBean(properties).getObject());
      map.from(properties::determineAddresses).to(factory::setAddresses);
      map.from(properties::isPublisherReturns).to(factory::setPublisherReturns);
      map.from(properties::getPublisherConfirmType)
          .whenNonNull()
          .to(factory::setPublisherConfirmType);
      RabbitProperties.Cache.Channel channel = properties.getCache().getChannel();
      map.from(channel::getSize).whenNonNull().to(factory::setChannelCacheSize);
      map.from(channel::getCheckoutTimeout)
          .whenNonNull()
          .as(Duration::toMillis)
          .to(factory::setChannelCheckoutTimeout);
      RabbitProperties.Cache.Connection connection = properties.getCache().getConnection();
      map.from(connection::getMode).whenNonNull().to(factory::setCacheMode);
      map.from(connection::getSize).whenNonNull().to(factory::setConnectionCacheSize);
      map.from(connectionNameStrategy::getIfUnique)
          .whenNonNull()
          .to(factory::setConnectionNameStrategy);
      return factory;
    }
  }

  private static void addAdvices(
      AbstractRabbitListenerContainerFactory<?> factory, Advice... advices) {
    Advice[] chain = factory.getAdviceChain();
    if (chain != null) {
      chain = Arrays.copyOf(chain, chain.length + advices.length);
      System.arraycopy(advices, 0, chain, factory.getAdviceChain().length, advices.length);
    } else {
      chain = advices;
    }
    factory.setAdviceChain(chain);
  }

  @Bean(name = "rabbitListenerContainerFactory")
  @ConditionalOnProperty(prefix = "spring.rabbitmq.listener", name = "type", havingValue = "direct")
  DirectRabbitListenerContainerFactory directRabbitListenerContainerFactory(
      DirectRabbitListenerContainerFactoryConfigurer configurer,
      ConnectionFactory connectionFactory,
      BeetleListenerInterceptor interceptor) {
    DirectRabbitListenerContainerFactory factory = new DirectRabbitListenerContainerFactory();
    configurer.configure(factory, connectionFactory);
    addAdvices(factory, interceptor);
    return factory;
  }

  @Bean(name = "rabbitListenerContainerFactory")
  @ConditionalOnProperty(
      prefix = "spring.rabbitmq.listener",
      name = "type",
      havingValue = "simple",
      matchIfMissing = true)
  SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory(
      SimpleRabbitListenerContainerFactoryConfigurer configurer,
      ConnectionFactory connectionFactory,
      BeetleListenerInterceptor interceptor) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    configurer.configure(factory, connectionFactory);
    addAdvices(factory, interceptor);
    return factory;
  }

  @Bean
  @ConditionalOnMissingBean
  Deduplicator deduplicator(
      KeyValueStore store, DeduplicationConfiguration deduplicationConfiguration) {
    return new KeyValueStoreBasedDeduplicator(store, deduplicationConfiguration);
  }

  @Bean
  @ConditionalOnMissingBean
  BeetleListenerInterceptor beetleListenerInterceptor(
      RabbitListenerEndpointRegistry registry,
      Deduplicator deduplicator,
      BeetleConnectionFactory factory) {
    return new BeetleListenerInterceptor(
        deduplicator, registry, factory.isInvertRequeueParameter());
  }
}
