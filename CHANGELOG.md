Changelog
=========

Version 1.2 (unreleased)
------------------------

Changes:



Version 1.1.0 (28.10.2020)
--------------------------

Changes:

* Lower Java runtime requirements to version 8
* Fix messages with missing message IDs
* Consistently use slf4j for logging in the library
* improve test execution speed
* Minimum required Spring version is now 2.2.1
* fix inconsistent versions used for testcontainers
* fix key cleanup in Redis deduplication store
* fix missing queue names in deduplictaion store key

Version 1.0.8 (02.10.2020)
--------------------------

Changes:

* fix int/long parsing from String headers.


Version 1.0.7 (24.08.2020)
--------------------------

Changes:

* Use Spring Boot 2.3.3.RELEASE version.
* separate spring-rabbit-test version from spring version.

Version 1.0.6 (17.06.2020)
--------------------------

Changes:

* support deduplication in Beetle core with basicPublish and basicConsume methods.
* support deduplication with Spring using @RabbitListener and not for RabbitTemplate.
* update documentation

Version 1.0.5 (08.06.2020)
--------------------------

Changes:

* add setters for BeetleAmqpConfiguration.
* add a sample app demonstrating use of Java Beetle Client without Spring configuration.

Version 1.0.4 (05.06.2020)
--------------------------

Changes:

* remove Spring Boot Application from spring-integration module.


Version 1.0.3 (04.06.2020)
--------------------------

Changes:

* fix expires_at header value comparison by using unix timestamps

Version 1.0.2 (04.06.2020)
--------------------------

Changes:

* add default expires_at header when publishing a message

Version 1.0.1 (29.05.2020)
--------------------------

Changes:

* add BOM pom

Version 1.0 (28.05.2020)
------------------------

Initial release










