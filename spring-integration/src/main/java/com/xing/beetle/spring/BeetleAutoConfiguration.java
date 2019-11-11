package com.xing.beetle.spring;

import com.rabbitmq.client.Channel;
import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.AbstractRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.amqp.DirectRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class BeetleAutoConfiguration {

    @Bean(name = "rabbitListenerContainerFactory")
    @ConditionalOnProperty(prefix = "spring.rabbitmq.listener", name = "type", havingValue = "simple",
            matchIfMissing = true)
    SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        setupBeetleAdvice(factory);
        return factory;
    }

    @Bean(name = "rabbitListenerContainerFactory")
    @ConditionalOnProperty(prefix = "spring.rabbitmq.listener", name = "type", havingValue = "direct")
    DirectRabbitListenerContainerFactory directRabbitListenerContainerFactory(
            DirectRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
        DirectRabbitListenerContainerFactory factory = new DirectRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        setupBeetleAdvice(factory);
        return factory;
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

    @Bean
    RabbitListenerInterceptor rabbitListenerInterceptor() {
        return new RabbitListenerInterceptor();
    }

    static class RabbitListenerInterceptor implements MethodInterceptor {

        @Override
        public Object invoke(MethodInvocation invocation) throws Throwable {
            try {
                Channel channel = (Channel) invocation.getArguments()[0];
                Message message = (Message) invocation.getArguments()[1];
                long deliveryTag = message.getMessageProperties().getDeliveryTag();
                //TODO: deduplication logic wil be applied from here
                if (deliveryTag % 2 == 0) {
                    channel.basicAck(deliveryTag, false);
                    return null;
                } else {
                    return invocation.proceed();
                }
            } finally {
                // post process
            }
        }

    }


}
