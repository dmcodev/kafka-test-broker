package dev.dmcode.test.kafka

import dev.dmcode.test.kafka.config.BrokerConfig
import dev.dmcode.test.kafka.config.TopicConfig
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.core.test.TestResult
import io.kotest.data.Row6
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
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import kotlin.random.Random

class ProducerConsumerSpec : StringSpec() {

    private val dispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()

    init {
        "Should produce and consume messages" {
            createBroker()
            val clientProperties = clientProperties()
            KafkaProducer<String, String>(clientProperties).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key1", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, "key2", "value2"))
                send(ProducerRecord(TEST_TOPIC_2, "key3", "value3"))
                send(ProducerRecord(TEST_TOPIC_2, null, "value4"))
                send(ProducerRecord(TEST_TOPIC_2, "key4", null))
                close()
            }
            KafkaConsumer<String, String>(clientProperties).run {
                subscribe(listOf(TEST_TOPIC_1, TEST_TOPIC_2))
                poll(Duration.ofSeconds(1)).also { close(Duration.ZERO) }
            }.verify(
                row(TEST_TOPIC_1, 0, 0, "key1", "value1", emptyList()),
                row(TEST_TOPIC_1, 0, 1, "key2", "value2", emptyList()),
                row(TEST_TOPIC_2, 0, 0, "key3", "value3", emptyList()),
                row(TEST_TOPIC_2, 0, 1, null, "value4", emptyList()),
                row(TEST_TOPIC_2, 0, 2, "key4", null, emptyList())
            )
        }

        "Should produce and consume messages with headers" {
            createBroker()
            val clientProperties = clientProperties()
            KafkaProducer<String, String>(clientProperties).apply {
                val record = ProducerRecord(TEST_TOPIC_1, "key", "value")
                record.headers().apply {
                    add("hk1", "hv1".toByteArray())
                    add("hk2", "hv2".toByteArray())
                    add("hk3", null)
                }
                send(record)
                close()
            }
            KafkaConsumer<String, String>(clientProperties).run {
                subscribe(listOf(TEST_TOPIC_1))
                poll(Duration.ofSeconds(1)).also { close() }
            }.verify(
                row(TEST_TOPIC_1, 0, 0, "key", "value", listOf("hk1" to "hv1", "hk2" to "hv2", "hk3" to null))
            )
        }

        "Should commit and resume consuming from last committed offset" {
            createBroker()
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
                poll(Duration.ofSeconds(3)).also { close() }
            }
            preCommitRecords.verify(
                row(TEST_TOPIC_1, 0, 0, "key1", "value1", emptyList()),
                row(TEST_TOPIC_2, 0, 0, "key2", "value2", emptyList())
            )
            afterCommitRecords.verify(
                row(TEST_TOPIC_1, 0, 1, "key3", "value3", emptyList()),
                row(TEST_TOPIC_2, 0, 1, "key4", "value4", emptyList())
            )
        }

        "Should produce and consume with many producers and consumers" {
            val producersCount = 2
            val consumersCount = 4
            val messagesCount = 10000
            val brokerConfig = BrokerConfig.builder()
                .topic(TopicConfig.create(TEST_TOPIC_1, 10))
                .build()
            val clientProperties = clientProperties(brokerConfig)
            val testMessages = (1 .. messagesCount).asSequence().map { "key$it" to "value$it" }.toList()
            val producerRecords = LinkedBlockingDeque(testMessages.map { ProducerRecord(TEST_TOPIC_1, it.first, it.second) })
            val startBarrier = CyclicBarrier(producersCount + consumersCount + 1)
            createBroker(brokerConfig)
            repeat(producersCount) {
                launch(dispatcher) {
                    startBarrier.await()
                    val producer = KafkaProducer<String, String>(clientProperties)
                    while (producerRecords.isNotEmpty()) {
                        val record = producerRecords.pollFirst() ?: break
                        producer.send(record)
                    }
                    producer.close()
                }
            }
            val consumedRecords = LinkedBlockingDeque<ConsumerRecord<String, String>>()
            repeat(consumersCount) {
                launch(dispatcher) {
                    startBarrier.await()
                    val consumer = KafkaConsumer<String, String>(clientProperties)
                    consumer.subscribe(listOf(TEST_TOPIC_1))
                    while (consumedRecords.size < testMessages.size) {
                        val records = consumer.poll(Duration.ofMillis(250))
                        try {
                            consumer.commitSync()
                        } catch (ex: RebalanceInProgressException) {
                            continue
                        }
                        records.forEach(consumedRecords::addLast)
                    }
                    consumer.close()
                }
            }
            startBarrier.await()
            await { consumedRecords.size >= testMessages.size }
            consumedRecords.size shouldBe testMessages.size
            val consumedData = consumedRecords.map { it.key()!! to it.value()!! }
            consumedData.toSet() shouldBe testMessages.toSet()
        }

        "Should handle consumer randomly joining, leaving, subscribing and unsubscribing" {
            val consumersActors = 10
            val messagesCount = 5000
            val topics = listOf(TEST_TOPIC_1, TEST_TOPIC_2)
            val brokerConfig = BrokerConfig.builder()
                .topic(TopicConfig.create(TEST_TOPIC_1, 5))
                .topic(TopicConfig.create(TEST_TOPIC_2, 5))
                .build()
            val clientProperties = clientProperties(brokerConfig)
            clientProperties["max.partition.fetch.bytes"] = 1024
            val testMessages = (1 .. messagesCount).asSequence().map { "key$it" to "value$it" }.toList()
            val startBarrier = CyclicBarrier(consumersActors + 2)
            createBroker(brokerConfig)
            launch(dispatcher) {
                startBarrier.await()
                val producer = KafkaProducer<String, String>(clientProperties)
                testMessages.forEach {
                    val topic = topics.random()
                    producer.send(ProducerRecord(topic, it.first, it.second))
                }
                producer.close()
            }
            val consumedRecords = LinkedBlockingDeque<ConsumerRecord<String, String>>()
            repeat(consumersActors) {
                launch(dispatcher) {
                    startBarrier.await()
                    var consumer = KafkaConsumer<String, String>(clientProperties)
                    consumer.subscribe(topics)
                    while (consumedRecords.size < messagesCount) {
                        if (Random.nextInt(5) == 0) {
                            consumer.close()
                            consumer = KafkaConsumer(clientProperties)
                            consumer.subscribe(topics)
                        }
                        if (Random.nextInt(5) == 0) {
                            consumer.unsubscribe()
                            consumer.subscribe(topics)
                        }
                        val records = consumer.poll(Duration.ofSeconds(1))
                        try {
                            consumer.commitSync()
                        } catch (ex: RebalanceInProgressException) {
                            continue
                        }
                        records.forEach(consumedRecords::addLast)
                    }
                    consumer.close()
                }
            }
            startBarrier.await()
            await { consumedRecords.size >= messagesCount }
            consumedRecords.size shouldBe messagesCount
            val consumedData = consumedRecords.map { it.key()!! to it.value()!! }
            consumedData.toSet() shouldBe testMessages.toSet()
        }

        "Should reset broker state" {
            val broker = createBroker()
            val topicQuery = broker.query().topic(TEST_TOPIC_1)
            val clientProperties = clientProperties()
            repeat(5) {
                KafkaProducer<String, String>(clientProperties).apply {
                    send(ProducerRecord(TEST_TOPIC_1, "key1", "value1"))
                    close()
                }
                KafkaConsumer<String, String>(clientProperties).run {
                    subscribe(listOf(TEST_TOPIC_1))
                    poll(Duration.ofSeconds(1)).also {
                        commitSync()
                        close()
                    }
                }.verify(
                    row(TEST_TOPIC_1, 0, 0, "key1", "value1", emptyList())
                )
                topicQuery.records().all().size shouldBe 1
                broker.reset()
                topicQuery.exists() shouldBe false
            }
        }

        "Should produce and consume using GZIP compression" {
            createBroker()
            val clientProperties = clientProperties()
            clientProperties["compression.type"] = "gzip"
            KafkaProducer<String, String>(clientProperties).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value"))
                close()
            }
            KafkaConsumer<String, String>(clientProperties).run {
                subscribe(listOf(TEST_TOPIC_1))
                poll(Duration.ofSeconds(1)).also { close() }
            }.verify(
                row(TEST_TOPIC_1, 0, 0, "key", "value", emptyList())
            )
        }
    }

    private val createdBrokers = mutableListOf<KafkaTestBroker>()

    override fun afterEach(testCase: TestCase, result: TestResult) {
        createdBrokers.forEach { it.close() }
        createdBrokers.clear()
    }

    override fun afterSpec(spec: Spec) {
        dispatcher.close()
    }

    private fun createBroker(config: BrokerConfig = BrokerConfig.createDefault()): KafkaTestBroker =
        KafkaTestBroker(config).also { createdBrokers.add(it) }

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
            it["heartbeat.interval.ms"] = "100"
            it["enable.auto.commit"] = "false"
        }

    private suspend fun await(timeoutMs: Long = 60_000, condition: () -> Boolean) {
        withTimeout(timeoutMs) {
            while (!condition()) {
                delay(25)
            }
        }
    }

    private fun ConsumerRecords<*, *>.verify(vararg records: Row6<String, Int, Int, String?, String?, List<Pair<String, String?>>>) {
        count() shouldBe records.size
        records.forEach { expectedRecord ->
            val record = asSequence()
                .filter { it.topic() == expectedRecord.a }
                .filter { it.partition() == expectedRecord.b }
                .filter { it.offset() == expectedRecord.c.toLong() }
                .firstOrNull()
                ?: throw AssertionError("Could not found record ${expectedRecord.a}/${expectedRecord.b}/${expectedRecord.c}")
            record.key() shouldBe expectedRecord.d
            record.value() shouldBe expectedRecord.e
            val headersMap = record.headers().associate { it.key() to it.value()?.let(::String) }
            headersMap.size shouldBe expectedRecord.f.size
            expectedRecord.f.forEach { expectedHeader ->
                val header = headersMap[expectedHeader.first]
                header shouldBe expectedHeader.second
            }
        }
    }

    companion object {
        private const val TEST_TOPIC_1 = "test-topic-1"
        private const val TEST_TOPIC_2 = "test-topic-2"
        private const val TEST_CONSUMER_GROUP = "test-consumer-group"
    }
}