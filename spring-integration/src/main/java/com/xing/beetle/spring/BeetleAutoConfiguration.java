package com.xing.beetle.spring;

import com.rabbitmq.client.Channel;
import com.xing.beetle.dedup.api.MessageListener;
import com.xing.beetle.dedup.spi.DedupStore;
import com.xing.beetle.dedup.spi.MessageAdapter;
import com.xing.beetle.util.ExceptionSupport;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        private DedupStore<String> store = new DedupStore.InMemoryStore<>();
        private SpringMessageAdaptor adaptor = new SpringMessageAdaptor();

        @SuppressWarnings("unchecked")
        @Override
        public Object invoke(MethodInvocation invocation) throws Throwable {
            Channel channel = (Channel) invocation.getArguments()[0];
            adaptor.setChannel(channel);
            Object data = invocation.getArguments()[1];
            boolean multiple = data instanceof List;
            List<Message> messages = multiple ? (List<Message>) data : new ArrayList<>(Collections.singletonList((Message) data));
            MessageListener<Message> listener = msg -> {
                invocation.getArguments()[1] = multiple ? Collections.singletonList(msg) : msg;
                invocation.proceed();
            };
            messages.forEach(msg -> store.find(msg.getMessageProperties().getMessageId()).handle(msg, listener)
                    .apply(adaptor, store, null));
            return null;
        }
    }

    static class SpringMessageAdaptor implements MessageAdapter<Message, String> {

        private Channel channel;

        @Override
        public void acknowledge(Message message) {
            try {
                this.channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            } catch (IOException e) {
                ExceptionSupport.sneakyThrow(e);
            }
        }

        @Override
        public String keyOf(Message message) {
            return message.getMessageProperties().getMessageId();
        }

        @Override
        public void requeue(Message message) {
            try {
                this.channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
            } catch (IOException e) {
                ExceptionSupport.sneakyThrow(e);
            }

        }

        public void setChannel(Channel channel) {
            this.channel = channel;
        }
    }
}
