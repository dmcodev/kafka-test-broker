package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import dev.dmco.test.kafka.config.TopicConfig
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.AtomicInteger


class KafkaProducerConsumerSpec : StringSpec({

    "Should send message to topic" {
        val topicName = "test-topic"
        val producersCount = 2
        val consumersCount = 6

        val brokerConfig = BrokerConfig.builder()
            .topic(TopicConfig.create(topicName, 10))
            .build()
        val broker = TestKafkaBroker(brokerConfig)

        val clientProperties = Properties()
        clientProperties["bootstrap.servers"] = "${brokerConfig.host()}:${brokerConfig.port()}"
        clientProperties["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        clientProperties["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        clientProperties["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        clientProperties["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        clientProperties["compression.type"] = "none"
        clientProperties["group.id"] = "test-consumer-group"
        clientProperties["heartbeat.interval.ms"] = "1000"
        clientProperties["auto.commit.interval.ms"] = "3000"
        clientProperties["enable.auto.commit"] = "false"


        val testData = (1 .. 50000).asSequence().map { "key$it" to "value$it" }.toList()

        val producerRecords = LinkedBlockingDeque(testData.map { ProducerRecord(topicName, it.first, it.second) })
        val dispatcher = Executors.newFixedThreadPool(producersCount + consumersCount).asCoroutineDispatcher()
        val closedConsumersCount = AtomicInteger(0)

        repeat(producersCount) {
            launch(dispatcher) {
                val producer = KafkaProducer<String, String>(clientProperties)
                while (producerRecords.isNotEmpty()) {
                    val record = producerRecords.pollFirst() ?: break
                    producer.send(record)
                }
                producer.close()
            }
        }

        val consumedRecords = LinkedBlockingDeque<ConsumerRecord<String, String>>()
        val consumerCounts = mutableMapOf<Int, Long>()

        repeat(consumersCount) {
            val consumerId = it
            launch(dispatcher) {
                val consumer = KafkaConsumer<String, String>(clientProperties)
                consumer.subscribe(listOf(topicName))
                while (consumedRecords.size != testData.size) {
                    val records = consumer.poll(Duration.ofSeconds(1))
                    consumerCounts[consumerId] = (consumerCounts[consumerId] ?: 0L) + records.count()
                    records.forEach(consumedRecords::addLast)
                }
                closedConsumersCount.incrementAndGet()
                consumer.close()
            }
        }

        while (closedConsumersCount.get() != consumersCount) {
            delay(100)
        }

        val consumedData = consumedRecords.map { it.key()!! to it.value()!! }

        consumedData.toSet() shouldBe testData.toSet()

        dispatcher.close()
        broker.close()
    }
})