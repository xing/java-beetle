package com.xing.beetle.redis;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.dedup.spi.KeyValueStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(BeetleRedisProperties.class)
public class RedisDedupStoreAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  KeyValueStore beetleDedupStore(
      BeetleRedisProperties properties, BeetleAmqpConfiguration beetleAmqpConfiguration) {
    return new RedisDedupStore(properties, beetleAmqpConfiguration);
  }
}
