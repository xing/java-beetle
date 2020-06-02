package com.xing.beetle.demo

import com.rabbitmq.client.AlreadyClosedException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.Declarables
import org.springframework.amqp.core.ExchangeBuilder
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import java.util.*
import kotlin.concurrent.thread

/**
 * This example simulates an asynchronous producer and an asynchronous receiver. Will keep going until stopped.
 */
fun main() {
    runApplication<Application>()
}

@SpringBootApplication
class Application

@Configuration
class AmqpConfiguration {
    @Bean
    fun amqpBindings() = Declarables(
        org.springframework.amqp.core.Queue(MY_QUEUE, false),
        ExchangeBuilder.directExchange("auto.exch").ignoreDeclarationExceptions().build(),
        Binding(MY_QUEUE, Binding.DestinationType.QUEUE, "auto.exch", "orderRoutingKey", null)
    )

    companion object {
        const val MY_QUEUE = "myQueue"
    }
}

@Component
class AmqpProducer(private val template: AmqpTemplate) : CommandLineRunner {
    private val logger: Logger = LoggerFactory.getLogger(AmqpProducer::class.java)

    override fun run(vararg args: String) {
        thread {
            var batchNum = 0
            while (true) {
                val message = Message(
                    ByteArray(0),
                    MessageProperties().apply { messageId = UUID.randomUUID().toString() }
                )

                try {
                    repeat(40) {
                        template.send(AmqpConfiguration.MY_QUEUE, message)
                    }
                    repeat(4) {
                        template.convertAndSend(AmqpConfiguration.MY_QUEUE, "hello world")
                    }

                    logger.info("Finished producing batch ${batchNum++}")

                    Thread.sleep(2000)
                } catch (e: Exception) {
                    logger.error("Exception while producing", e)
                }
            }
        }
    }
}

@Component
class AmqpReceiverWithTemplate(private val template: AmqpTemplate) : CommandLineRunner {
    private val logger: Logger = LoggerFactory.getLogger(AmqpReceiverWithTemplate::class.java)

    override fun run(vararg args: String?) {
        thread {
            while (true) {
                try {
                    template.receive(AmqpConfiguration.MY_QUEUE, 500)
                        ?.toString()
                        .let { logger.info(it) }
                } catch (e: Exception) {
                    if (e.cause is AlreadyClosedException) {
                        return@thread
                    }
                    logger.error("Exception while receiving", e)
                }
            }
        }
    }
}
