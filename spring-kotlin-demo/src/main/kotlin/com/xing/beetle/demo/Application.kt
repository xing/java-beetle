package com.xing.beetle.demo

import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.ExitCodeGenerator
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import java.util.*

fun main() {
    runApplication<Application>()
}

@SpringBootApplication
class Application

@Component
class AmqpDemo(
    private val appContext: ApplicationContext,
    private val template: AmqpTemplate
) : CommandLineRunner {
    override fun run(vararg args: String) {
        val message = Message(
            ByteArray(0),
            MessageProperties().apply { messageId = UUID.randomUUID().toString() }
        )

        repeat(40) {
            template.send("myQueue", message)
        }
        repeat(4) {
            template.convertAndSend("myQueue", "hello world")
        }
        repeat(44) {
            println(template.receive("myQueue", 50))
        }

        SpringApplication.exit(appContext, ExitCodeGenerator { 0 })
    }
}
