package com.xing.beetle.redis;

import com.xing.beetle.amqp.BeetleAmqpConfiguration;
import com.xing.beetle.dedup.spi.KeyValueStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisDedupStoreAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  KeyValueStore beetleDedupStore(BeetleAmqpConfiguration beetleAmqpConfiguration) {
    return new RedisDedupStore(beetleAmqpConfiguration);
  }
}
