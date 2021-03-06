package com.kontofahren.integrationslosung

import com.google.gson.Gson
import com.kontofahren.integrationslosung.Exchange.AUDIT_EXCHANGE
import com.kontofahren.integrationslosung.Exchange.INVOICE_EXCHANGE
import com.kontofahren.integrationslosung.Exchange.LOCATION_EXCHANGE
import com.kontofahren.integrationslosung.Exchange.LOG_EXCHANGE
import com.kontofahren.integrationslosung.Queue.AUDIT_SAVE
import com.kontofahren.integrationslosung.Routing.EMPTY
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.BuiltinExchangeType.FANOUT
import com.rabbitmq.client.BuiltinExchangeType.TOPIC
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope

class RabbitGateway(
        val username: String = "user",
        val password: String = "pass",
        val vhost: String = "vhost",
        val host: String = "mq",
        val port: Int = 5672
) {
    /**
     * Factory for connections to
     */
    private val factory by lazy {
        ConnectionFactory().apply {
            username = this@RabbitGateway.username
            password = this@RabbitGateway.password
            virtualHost = this@RabbitGateway.vhost
            host = this@RabbitGateway.host
            port = this@RabbitGateway.port
        }
    }

    private val connection by lazy {
        this.factory.newConnection()
    }

    private val channel by lazy {
        connection.createChannel()
    }

    init {
        exchangeDeclare(Exchange.LOCATION_EXCHANGE, FANOUT)
        exchangeDeclare(LOG_EXCHANGE, TOPIC)
        exchangeDeclare(AUDIT_EXCHANGE, FANOUT)
        exchangeDeclare(INVOICE_EXCHANGE, FANOUT)

        queueDeclare(Queue.FRONTEND_LOCATION_UPDATE)
        queueDeclare(Queue.LOCATION_TO_ACTIVITY)
        queueDeclare(Queue.AUDIT_SAVE)
        queueDeclare(Queue.INVOICE_GENERATION)
        queueDeclare(Queue.INVOICE_REGENERATION)

        queueBind(Queue.FRONTEND_LOCATION_UPDATE, LOCATION_EXCHANGE, EMPTY)
        queueBind(Queue.LOCATION_TO_ACTIVITY, LOCATION_EXCHANGE, EMPTY)
        queueBind(Queue.AUDIT_SAVE, AUDIT_EXCHANGE, EMPTY)
        queueBind(Queue.INVOICE_GENERATION, INVOICE_EXCHANGE, EMPTY)
        queueBind(Queue.INVOICE_REGENERATION, INVOICE_EXCHANGE, EMPTY)
    }

    private fun exchangeDeclare(exchange: Exchange, type: BuiltinExchangeType) = try {
        channel.exchangeDeclare(exchange.name, type)
    } catch (e: Exception) {
        println("Could not declare the exchange.")
    }
    private fun queueDeclare(queue: Queue, durable: Boolean = true, exclusive: Boolean = false, autoDelete: Boolean = false, config: Map<String, String> = emptyMap()) = try {
        channel.queueDeclare(queue.name, durable, exclusive, autoDelete, config)
    } catch(e: Exception) {
        println("Could not declare the queue")
    }
    private fun queueBind(queue: Queue, exchange: Exchange, routing: Routing) = try {
        channel.queueBind(queue.name, exchange.name, routing.name)
    } catch (e: Exception) {
        println("Could not bind to queue")
    }

    /**
     * Attach a handler function to a queue
     * @param queue Name of the Queue to attach to
     * @param handler handler function that receives a JSON obj
     *
     */
    fun consume(queue: String, handler: (String) -> Unit) {
        try {
            val consumer = object : DefaultConsumer(channel) {
                override fun handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: ByteArray) {
                    val tag = envelope.deliveryTag
                    try {
                        handler(body.toString(Charsets.UTF_8))
                        channel.basicAck(tag, false)
                    } catch (ex: Exception) {
                        println(ex)
                    }
                }
            }

            channel.basicConsume(queue, false, consumer)
        } catch (e: Exception) {
            println("Could not consume")
        }
    }

    /**
     * @see RabbitGateway.consume
     */
    fun consume(queue: Queue, handler: (String) -> Unit) = consume(queue.name, handler)

    /**
     *  Create a one off queue in an exchange
     */
    fun createExclusiveQueue(exchange: Exchange, routing: Routing = Routing.EMPTY) = channel.queueDeclare().apply {
        channel.queueBind(queue, exchange.name, routing.name)
    }.queue

    /**
     * Publish a message to an exchange
     * @param exchange Name of the exchange
     * @param obj Object  to encode as JSON in body
     * @param routing Routing to assign
     * @param deliveryMode mode of delivery. Default is persist
     */
    fun publish(
            exchange: String,
            obj: Any,
            routing: String,
            deliveryMode: Int = 2
    ) {
        try {
            val props = BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(deliveryMode)
                    .build()
            val json = Gson().toJson(obj)
            channel.basicPublish(exchange, routing, props, json.toByteArray(Charsets.UTF_8))
        } catch(e: Exception) {
            println("Could not publish")
        }
    }

    /**
     * @see publish
     */
    fun publish(
            exchange: Exchange,
            obj: Any,
            routing: Routing,
            deliverMode: Int = 2
    ) = publish(exchange.name, obj, routing.name, deliverMode)
}

enum class Exchange {
    LOCATION_EXCHANGE,
    AUDIT_EXCHANGE,
    LOG_EXCHANGE,
    INVOICE_EXCHANGE,
}

enum class Queue {
    FRONTEND_LOCATION_UPDATE,
    LOCATION_TO_ACTIVITY,
    AUDIT_SAVE,
    INVOICE_GENERATION,
    INVOICE_REGENERATION
}

enum class Routing {
    EMPTY,
    ERROR,
    CREATE,
}
