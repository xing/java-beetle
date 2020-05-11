package com.xing.beetle.demo

import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.annotation.Exchange
import org.springframework.amqp.rabbit.annotation.Queue
import org.springframework.amqp.rabbit.annotation.QueueBinding
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import java.util.*

@SpringBootApplication
class Application {
    @RabbitListener(bindings = [QueueBinding(value = Queue(value = "myQueue", durable = "false"),
            exchange = Exchange(value = "auto.exch", ignoreDeclarationExceptions = "true"), key = ["orderRoutingKey"])], ackMode = "AUTO")
    fun processOrder(message: Message?) {
        println(message)
    }

    companion object {
        @Throws(InterruptedException::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val context = SpringApplication.run(Application::class.java, *args)
            val template = context.getBean(AmqpTemplate::class.java)
            val props = MessageProperties()
            props.messageId = UUID.randomUUID().toString()
            val message = Message(ByteArray(0), props)
            for (i in 0..39) {
                template.send("myQueue", message)
            }
            for (i in 0..3) {
                template.convertAndSend("myQueue", "hello world")
            }
            Thread.sleep(2000)
            context.close()
        }
    }
}
