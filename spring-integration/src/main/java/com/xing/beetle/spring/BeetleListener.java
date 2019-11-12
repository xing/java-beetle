package com.xing.beetle.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.messaging.handler.annotation.MessageMapping;

@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Repeatable(BeetleListeners.class)
public @interface BeetleListener {

  /**
   * The unique identifier of the container managing for this endpoint.
   *
   * <p>If none is specified an auto-generated one is provided.
   *
   * @return the {@code id} for the container managing for this endpoint.
   * @see
   *     org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry#getListenerContainer(String)
   */
  String id() default "";

  /**
   * The bean name of the {@link
   * org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory} to use to create the
   * message listener container responsible to serve this endpoint.
   *
   * <p>If not specified, the default container factory is used, if any.
   *
   * @return the {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}
   *     bean name.
   */
  String containerFactory() default "";

  /**
   * The queues for this listener. The entries can be 'queue name', 'property-placeholder keys' or
   * 'expressions'. Expression must be resolved to the queue name or {@code Queue} object. The
   * queue(s) must exist, or be otherwise defined elsewhere as a bean(s) with a {@link
   * org.springframework.amqp.rabbit.core.RabbitAdmin} in the application context. Mutually
   * exclusive with {@link #bindings()} and {@link #queuesToDeclare()}.
   *
   * @return the queue names or expressions (SpEL) to listen to from target
   * @see org.springframework.amqp.rabbit.listener.MessageListenerContainer
   */
  String[] queues() default {};

  /**
   * The queues for this listener. If there is a {@link
   * org.springframework.amqp.rabbit.core.RabbitAdmin} in the application context, the queue will be
   * declared on the broker with default binding (default exchange with the queue name as the
   * routing key). Mutually exclusive with {@link #bindings()} and {@link #queues()}.
   *
   * @return the queue(s) to declare.
   * @see org.springframework.amqp.rabbit.listener.MessageListenerContainer
   * @since 2.0
   */
  Queue[] queuesToDeclare() default {};

  /**
   * When {@code true}, a single consumer in the container will have exclusive use of the {@link
   * #queues()}, preventing other consumers from receiving messages from the queues. When {@code
   * true}, requires a concurrency of 1. Default {@code false}.
   *
   * @return the {@code exclusive} boolean flag.
   */
  boolean exclusive() default false;

  /**
   * The priority of this endpoint. Requires RabbitMQ 3.2 or higher. Does not change the container
   * priority by default. Larger numbers indicate higher priority, and both positive and negative
   * numbers can be used.
   *
   * @return the priority for the endpoint.
   */
  String priority() default "";

  /**
   * Reference to a {@link org.springframework.amqp.rabbit.core.RabbitAdmin RabbitAdmin}. Required
   * if the listener is using auto-delete queues and those queues are configured for conditional
   * declaration. This is the admin that will (re)declare those queues when the container is
   * (re)started. See the reference documentation for more information.
   *
   * @return the {@link org.springframework.amqp.rabbit.core.RabbitAdmin} bean name.
   */
  String admin() default "";

  /**
   * Array of {@link QueueBinding}s providing the listener's queue names, together with the exchange
   * and optional binding information. Mutually exclusive with {@link #queues()} and {@link
   * #queuesToDeclare()}.
   *
   * @return the bindings.
   * @see org.springframework.amqp.rabbit.listener.MessageListenerContainer
   * @since 1.5
   */
  QueueBinding[] bindings() default {};

  /**
   * If provided, the listener container for this listener will be added to a bean with this value
   * as its name, of type {@code Collection<MessageListenerContainer>}. This allows, for example,
   * iteration over the collection to start/stop a subset of containers.
   *
   * @return the bean name for the group.
   * @since 1.5
   */
  String group() default "";

  /**
   * Set to "true" to cause exceptions thrown by the listener to be sent to the sender using normal
   * {@code replyTo/@SendTo} semantics. When false, the exception is thrown to the listener
   * container and normal retry/DLQ processing is performed.
   *
   * @return true to return exceptions. If the client side uses a {@code
   *     RemoteInvocationAwareMessageConverterAdapter} the exception will be re-thrown. Otherwise,
   *     the sender will receive a {@code RemoteInvocationResult} wrapping the exception.
   * @since 2.0
   */
  String returnExceptions() default "";

  /**
   * Set an {@link org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler} to
   * invoke if the listener method throws an exception. A simple String representing the bean name.
   * If a Spel expression (#{...}) is provided, the expression must evaluate to a bean name or a
   * {@link org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler} instance.
   *
   * @return the error handler.
   * @since 2.0
   */
  String errorHandler() default "";

  /**
   * Set the concurrency of the listener container for this listener. Overrides the default set by
   * the listener container factory. Maps to the concurrency setting of the container type.
   *
   * <p>For a {@link org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
   * SimpleMessageListenerContainer} if this value is a simple integer, it sets a fixed number of
   * consumers in the {@code concurrentConsumers} property. If it is a string with the form {@code
   * "m-n"}, the {@code concurrentConsumers} is set to {@code m} and the {@code
   * maxConcurrentConsumers} is set to {@code n}.
   *
   * <p>For a {@link org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer
   * DirectMessageListenerContainer} it sets the {@code consumersPerQueue} property.
   *
   * @return the concurrency.
   * @since 2.0
   */
  String concurrency() default "";

  /**
   * Set to true or false, to override the default setting in the container factory.
   *
   * @return true to auto start, false to not auto start.
   * @since 2.0
   */
  String autoStartup() default "";
}
