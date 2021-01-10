package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import dev.dmco.test.kafka.config.TopicConfig
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.core.test.TestResult
import io.kotest.data.Row5
import io.kotest.data.row
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.RebalanceInProgressException
import java.time.Duration
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque


class KafkaProducerConsumerSpec : StringSpec() {

    private val executor = Executors.newCachedThreadPool()
    private val dispatcher = executor.asCoroutineDispatcher()

    lateinit var broker: TestKafkaBroker

    init {
        "Should produce and consume messages" {
            broker = TestKafkaBroker()
            val clientProperties = clientProperties()
            KafkaProducer<String, String>(clientProperties).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key1", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, "key2", "value2"))
                send(ProducerRecord(TEST_TOPIC_2, "key3", "value3"))
                close()
            }
            KafkaConsumer<String, String>(clientProperties).run {
                subscribe(listOf(TEST_TOPIC_1, TEST_TOPIC_2))
                poll(Duration.ofSeconds(1)).also { close(Duration.ZERO) }
            }.verify(
                row(TEST_TOPIC_1, 0, 0, "key1", "value1"),
                row(TEST_TOPIC_1, 0, 1, "key2", "value2"),
                row(TEST_TOPIC_2, 0, 0, "key3", "value3")
            )
        }

        "Should commit and resume consuming from last committed offset" {
            broker = TestKafkaBroker()
            val clientProperties = clientProperties()
            val producer = KafkaProducer<String, String>(clientProperties).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key1", "value1")).get()
                send(ProducerRecord(TEST_TOPIC_2, "key2", "value2")).get()
            }
            val preCommitRecords = KafkaConsumer<String, String>(clientProperties).run {
                subscribe(listOf(TEST_TOPIC_1, TEST_TOPIC_2))
                poll(Duration.ofSeconds(1)).also {
                    commitSync()
                    close()
                }
            }
            producer.apply {
                send(ProducerRecord(TEST_TOPIC_1, "key3", "value3"))
                send(ProducerRecord(TEST_TOPIC_2, "key4", "value4"))
                close()
            }
            val afterCommitRecords = KafkaConsumer<String, String>(clientProperties).run {
                subscribe(listOf(TEST_TOPIC_1, TEST_TOPIC_2))
                poll(Duration.ofSeconds(1)).also { close(Duration.ZERO) }
            }
            preCommitRecords.verify(
                row(TEST_TOPIC_1, 0, 0, "key1", "value1"),
                row(TEST_TOPIC_2, 0, 0, "key2", "value2")
            )
            afterCommitRecords.verify(
                row(TEST_TOPIC_1, 0, 1, "key3", "value3"),
                row(TEST_TOPIC_2, 0, 1, "key4", "value4")
            )
        }

        "Should produce and consume with many producers and consumers" {
            val producersCount = 2
            val consumersCount = 4
            val messagesCount = 1000
            val brokerConfig = BrokerConfig.builder()
                .topic(TopicConfig.create(TEST_TOPIC_1, 10))
                .build()
            val clientProperties = clientProperties(brokerConfig)
            val testMessages = (1 .. messagesCount).asSequence().map { "key$it" to "value$it" }.toList()
            val producerRecords = LinkedBlockingDeque(testMessages.map { ProducerRecord(TEST_TOPIC_1, it.first, it.second) })
            broker = TestKafkaBroker(brokerConfig)
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
            var commitErrors = 0
            repeat(consumersCount) {
                launch(dispatcher) {
                    val consumer = KafkaConsumer<String, String>(clientProperties)
                    consumer.subscribe(listOf(TEST_TOPIC_1))
                    while (consumedRecords.size < testMessages.size) {
                        val records = consumer.poll(Duration.ofMillis(250))
                        try {
                            consumer.commitSync()
                        } catch (ex: RebalanceInProgressException) {
                            commitErrors++
                            continue
                        }
                        records.forEach(consumedRecords::addLast)
                    }
                    consumer.close(Duration.ZERO)
                }
            }
            await { consumedRecords.size >= testMessages.size }
            consumedRecords.size shouldBe  testMessages.size
            val consumedData = consumedRecords.map { it.key()!! to it.value()!! }
            consumedData.toSet() shouldBe testMessages.toSet()
            println(commitErrors)
        }
    }

    override fun afterEach(testCase: TestCase, result: TestResult) {
        if (this::broker.isInitialized) {
            broker.close()
        }
    }

    override fun afterSpec(spec: Spec) {
        dispatcher.close()
        executor.shutdownNow()
    }

    private fun clientProperties(
        config: BrokerConfig = BrokerConfig.createDefault()
    ): Properties =
        Properties().also {
            it["bootstrap.servers"] = "${config.host()}:${config.port()}"
            it["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
            it["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
            it["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            it["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            it["compression.type"] = "none"
            it["group.id"] = TEST_CONSUMER_GROUP
            it["heartbeat.interval.ms"] = "500"
            it["enable.auto.commit"] = "false"
        }

    private suspend fun await(timeoutMs: Long = 5000, condition: () -> Boolean) {
        withTimeout(timeoutMs) {
            while (!condition()) {
                delay(25)
            }
        }
    }

    private fun ConsumerRecords<*, *>.verify(vararg records: Row5<String, Int, Int, String, String>) {
        count() shouldBe records.size
        records.forEach { expected ->
            any {
                it.topic() == expected.a
                    && it.partition() == expected.b
                    && it.offset() == expected.c.toLong()
                    && it.key() == expected.d
                    && it.value() == expected.e
            } shouldBe true
        }
    }

    companion object {
        private const val TEST_TOPIC_1 = "test-topic-1"
        private const val TEST_TOPIC_2 = "test-topic-2"
        private const val TEST_CONSUMER_GROUP = "test-consumer-group"
    }
}