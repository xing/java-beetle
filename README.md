Beetle [![Build Status](https://travis-ci.org/xing/java-beetle.svg?branch=master)](https://travis-ci.org/xing/java-beetle)
======

High Availability AMQP Messaging with Redundant Queues

Java client library.

This library enables sending redundant messages to multiple AMQP brokers each having a queue.
This way, if one of the brokers crashes, the messages in the queue which are on the other broker will still be there.
At the receiving side, the beetle client will deduplicate the messages and the handler will receive the message once
(in case of successful handling).

This package consists of two parts. 
* The [beetle-core](./beetle-core) library 
* a [spring-integration](./spring-integration) for easy integration with spring applications.

It also contains some demo apps:
* [java beetle core demo](./beetle-core-demo) 
* [java spring demo](./spring-java-demo) 
* [kotlin spring demo](./spring-kotlin-demo).


How to use
----------

###### Beetle Core

Beetle Java Client, supports sending redundant messages via `BeetleChannel::basicPublish` when the Beetle header `x-publish-message-redundancy` is set to a value larger than `1`,
as seen in [beetle core demo Application](./beetle-core-demo/src/main/java/com/xing/beetle/demo/core/Application.java).

`x-publish-message-redundancy = 2` means, the message will be sent to `2` brokers if there are at least `2`. The list of RabbitMQ servers should be configured using `BEETLE_SERVERS` environment variable.

On the receiving side, deduplication is only supported for `BeetleChannel::basicConsume`, as it creates a consumer and subscribes to the queue for message consumption. Using `basicGet` will result
duplicates so it should not be used.

###### Spring Integration

Spring Integration module defines a `BeetleConnectionFactory` bean (as `@ConditionalOnMissingBean`) extending from `com.rabbitmq.client.ConnectionFactory`, so
by default Beetle Client will be used to communicate with RabbitMQ brokers unless another `ConnectionFactory` is configured.

The list of RabbitMQ servers should be configured using `beetle.servers` application property or `BEETLE_SERVERS` environment variable.

Please see the following demo apps for sample usage: 

* [java spring demo](./spring-java-demo) 
* [kotlin spring demo](./spring-kotlin-demo)

Important Note: Deduplication for Spring integration is only supported when `@RabbitListener` is used. Usage of `RabbitTemplate` for receiving messages is not supported. Indeed, `RabbitTemplate` abuses
consumers by creating a consumer for each message and closing them afterwards, which contradicts the purpose of a consumer. It also adds the overhead of establishing and tearing down the TCP connection every time.
Under high load this approach would risk to run into issues with the number of connections/sockets available to the application because the operating system might not immediately free the resources used.


For other configuration parameters see:
https://source.xing.com/gems/xing-amqp#deployment-considerations

#### Requeue Messages at the End of Queues

Please see: https://github.com/xing/beetle/blob/master/DEAD_LETTERING.md
