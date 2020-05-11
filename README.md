Beetle
======

High Availability AMQP Messaging with Redundant Queues

Java client library.

This library enables sending redundant messages to multiple AMQP brokers each having a queue.
This way, if one of the brokers crashes, the messages in the queue which are on the other broker will still be there.
At the receiving side, the beetle client will deduplicate the messages and the handler will receive the message once
(in case of successful handling). 

How to use
----------

This is work in progress, check back for an initial version.

###### Spring Integration

Spring users can directly use the provided Spring Integration dependency by adding it to their dependencies (`pom.xml` or `gradle.build` for instance).

The list of RabbitMQ servers should be configured using `beetle.servers` application property or `BEETLE_SERVERS` environment variable.

Example:

`beetle.servers=localhost:5672,localhost:5673`

For other configuration parameters see:
https://source.xing.com/gems/xing-amqp#deployment-considerations

Spring Integration module defines a `BeetleConnectionFactory` bean (as `@ConditionalOnMissingBean`) extending from `com.rabbitmq.client.ConnectionFactory`, so
by default Beetle Client will be used to communicate with RabbitMQ brokers unless another `ConnectionFactory` is configured.
