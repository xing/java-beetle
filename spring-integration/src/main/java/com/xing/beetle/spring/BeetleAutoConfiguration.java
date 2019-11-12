package com.xing.beetle.spring;

import com.rabbitmq.client.Channel;
import com.xing.beetle.amqp.BeetleConnectionFactory;
import com.xing.beetle.dedup.MessageHandlingState;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.KeyValueStore;
import com.xing.beetle.dedup.spi.MessageAdapter;
import com.xing.beetle.spring.BeetleAutoConfiguration.BeetleConnectionFactoryCreator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.AbstractRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.amqp.DirectRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(BeetleConnectionFactoryCreator.class)
public class BeetleAutoConfiguration {

  @Configuration
  static class BeetleConnectionFactoryCreator {

    @Bean
    BeetleConnectionFactory beetleConnectionFactory() {
      return new BeetleConnectionFactory();
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

  static class RabbitListenerInterceptor implements MethodInterceptor {

    private final KeyValueStore<String> store = new KeyValueStore.InMemoryStore();
    private final SpringMessageAdaptor adaptor = new SpringMessageAdaptor();

    @SuppressWarnings("unchecked")
    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
      Channel channel = (Channel) invocation.getArguments()[0];
      adaptor.setChannel(channel);
      Object data = invocation.getArguments()[1];
      boolean multiple = data instanceof List;
      List<Message> messages =
          multiple
              ? (List<Message>) data
              : new ArrayList<>(Collections.singletonList((Message) data));
      MessageListener<Message> listener =
          msg -> {
            invocation.getArguments()[1] = multiple ? Collections.singletonList(msg) : msg;
            invocation.proceed();
          };
      KeyValueStore<MessageHandlingState.Status> statusStore =
          store.suffixed(
              "status",
              MessageHandlingState.Status::valueOf,
              MessageHandlingState.Status::toString);
      messages.forEach(
          msg ->
              statusStore
                  .get(msg.getMessageProperties().getMessageId())
                  .orElse(MessageHandlingState.Status.INCOMPLETE)
                  .handle(msg, listener)
                  .apply(adaptor, store, null));
      return null;
    }
  }

  static class SpringMessageAdaptor implements MessageAdapter<Message> {

    private Channel channel;

    @Override
    public void acknowledge(Message message) {
      // done by spring auto ack
    }

    @Override
    public String keyOf(Message message) {
      return message.getMessageProperties().getMessageId();
    }

    @Override
    public void requeue(Message message) {
      // done by spring auto ack
    }

    public void setChannel(Channel channel) {
      this.channel = channel;
    }
  }

  @Bean(name = "rabbitListenerContainerFactory")
  @ConditionalOnProperty(prefix = "spring.rabbitmq.listener", name = "type", havingValue = "direct")
  DirectRabbitListenerContainerFactory directRabbitListenerContainerFactory(
      DirectRabbitListenerContainerFactoryConfigurer configurer,
      ConnectionFactory connectionFactory) {
    DirectRabbitListenerContainerFactory factory = new DirectRabbitListenerContainerFactory();
    configurer.configure(factory, connectionFactory);
    setupBeetleAdvice(factory);
    return factory;
  }

  @Bean
  RabbitListenerInterceptor rabbitListenerInterceptor() {
    return new RabbitListenerInterceptor();
  }

  private void setupBeetleAdvice(AbstractRabbitListenerContainerFactory<?> factory) {
    Advice[] adviceChain = factory.getAdviceChain();
    if (adviceChain == null) {
      adviceChain = new Advice[0];
    }
    adviceChain = Arrays.copyOf(adviceChain, adviceChain.length + 1);
    adviceChain[adviceChain.length - 1] = rabbitListenerInterceptor();
    factory.setAdviceChain(adviceChain);
  }

  @Bean(name = "rabbitListenerContainerFactory")
  @ConditionalOnProperty(
      prefix = "spring.rabbitmq.listener",
      name = "type",
      havingValue = "simple",
      matchIfMissing = true)
  SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory(
      SimpleRabbitListenerContainerFactoryConfigurer configurer,
      ConnectionFactory connectionFactory) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    configurer.configure(factory, connectionFactory);
    setupBeetleAdvice(factory);
    return factory;
  }
}
