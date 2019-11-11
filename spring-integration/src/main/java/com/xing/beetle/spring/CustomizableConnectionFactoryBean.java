package com.xing.beetle.spring;

import java.lang.reflect.Field;
import java.util.Objects;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import com.rabbitmq.client.ConnectionFactory;
import com.xing.beetle.util.ExceptionSupport;

public class CustomizableConnectionFactoryBean extends RabbitConnectionFactoryBean {

  public CustomizableConnectionFactoryBean(ConnectionFactory factory) {
    try {
      Field connectionFactory =
          RabbitConnectionFactoryBean.class.getDeclaredField("connectionFactory");
      connectionFactory.setAccessible(true);
      connectionFactory.set(this, Objects.requireNonNull(factory));
    } catch (Exception e) {
      ExceptionSupport.sneakyThrow(e);
    }
  }
}
