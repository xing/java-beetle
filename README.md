Beetle
======

High Availability AMQP Messaging with Redundant Queues

Java client library.

This package consists of there parts. The [beetle-core](https://github.com/xing/java-beetle/tree/master/beetle-core)  library, redis-dedup-store implementing message deduplication using redis
and a [spring-adapter](https://github.com/xing/java-beetle/tree/master/spring-integration) for easy integration with spring boot applications.

It also contains two sample spring boot projects in [java](https://github.com/xing/java-beetle/tree/master/spring-java-demo) and in [kotlin](https://github.com/xing/java-beetle/tree/master/spring-kotlin-demo).

This library enables sending redundant messages to multiple AMQP brokers each having a queue.
This way, if one of the brokers crashes, the messages in the queue which are on the other broker will still be there.
At the receiving side, the beetle client will deduplicate the messages and the handler will receive the message once
(in case of successful handling).

How to use
----------

###### Spring Integration

Spring users can directly use the provided Spring Integration [dependency](https://nexus.dc.xing.com/#browse/browse:sysarch-snapshots:com%2Fxing%2Fbeetle) by adding it to their dependencies (`pom.xml` or `gradle.build` for instance).

The list of RabbitMQ servers should be configured using `beetle.servers` application property or `BEETLE_SERVERS` environment variable.

Example:

`beetle.servers=localhost:5672,localhost:5673`




Spring Integration module defines a `BeetleConnectionFactory` bean (as `@ConditionalOnMissingBean`) extending from `com.rabbitmq.client.ConnectionFactory`, so
by default Beetle Client will be used to communicate with RabbitMQ brokers unless another `ConnectionFactory` is configured.
