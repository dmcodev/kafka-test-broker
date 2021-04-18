package dev.dmcode.test.kafka

import dev.dmcode.test.kafka.config.BrokerConfig
import dev.dmcode.test.kafka.config.TopicConfig
import dev.dmcode.test.kafka.state.query.deserializer.RecordDeserializer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.core.test.TestResult
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import java.util.concurrent.ExecutionException

class BrokerQuerySpec : StringSpec() {

    init {
        "Should reject query executed after broker shutdown" {
            val broker = createBroker()
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value"))
                close()
            }
            val query = broker.query().topic(TEST_TOPIC_1)
            broker.close()
            val error = shouldThrow<ExecutionException> { query.exists() }
            error.cause.shouldBeInstanceOf<IllegalStateException>()
            error.cause?.message shouldBe "Broker is closed"
        }

        "Should reuse query" {
            val broker = createBroker()
            val topic = broker.query().topic(TEST_TOPIC_1)
            topic.exists() shouldBe false
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value"))
                close()
            }
            topic.exists() shouldBe true
        }

        "Should query topic existence" {
            val broker = createBroker()
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value"))
                close()
            }
            broker.query().topic(TEST_TOPIC_1).exists() shouldBe true
            broker.query().topic(TEST_TOPIC_2).exists() shouldBe false
        }

        "Should query number of partitions" {
            val config = BrokerConfig.builder()
                .topic(TopicConfig.create(TEST_TOPIC_1, 3))
                .build()
            val broker = createBroker(config)
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value"))
                send(ProducerRecord(TEST_TOPIC_2, "key", "value"))
                close()
            }
            broker.query().topic(TEST_TOPIC_1).numberOfPartitions() shouldBe 3
            broker.query().topic(TEST_TOPIC_2).numberOfPartitions() shouldBe 1
        }

        "Should filter records by key" {
            val broker = createBroker()
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, "key_2", "value2"))
                send(ProducerRecord(TEST_TOPIC_1, null, "value3"))
                close()
            }
            val records = broker.query()
                .topic(TEST_TOPIC_1)
                .records()
                .keyDeserializer(RecordDeserializer.string())
            with(records.keyMatching { it?.endsWith("y") == true }.single()) {
                partitionId() shouldBe 0
                offset() shouldBe 0
                String(value()) shouldBe "value1"
            }
            with(records.keyMatching { it?.endsWith("2") == true }.single()) {
                partitionId() shouldBe 0
                offset() shouldBe 1
                String(value()) shouldBe "value2"
            }
            with(records.keyMatching { it == null }.single()) {
                partitionId() shouldBe 0
                offset() shouldBe 2
                String(value()) shouldBe "value3"
            }
            shouldThrow<IllegalStateException> { records.keyMatching { it?.startsWith("key") == true }.single() }
                .message shouldContain "Multiple matching records found"
            shouldThrow<IllegalStateException> { records.keyMatching { it == "non_existing" }.single() }
                .message shouldContain "No matching record found"
            records.keyMatching { it == "non_existing" }.all().isEmpty() shouldBe true
        }

        "Should filter records by value" {
            val broker = createBroker()
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key1", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, "key2", null))
                send(ProducerRecord(TEST_TOPIC_1, null, "value3"))
                close()
            }
            val records = broker.query()
                .topic(TEST_TOPIC_1)
                .records()
                .valueDeserializer(RecordDeserializer.string())
            with(records.valueMatching { it?.endsWith("3") == true }.single()) {
                offset() shouldBe 2
                key() shouldBe null
            }
            with(records.valueMatching { it == null }.single()) {
                offset() shouldBe 1
                String(key()) shouldBe "key2"
            }
            with(records.valueMatching { it?.startsWith("val") == true }.all()) {
                size shouldBe 2
            }
        }

        "Should filter records by header key" {
            val broker = createBroker()
            KafkaProducer<String, String>(clientProperties()).apply {
                send(
                    ProducerRecord(TEST_TOPIC_1, "key_1", "value_1").apply {
                        headers().apply {
                            add("hk", "hv".toByteArray())
                            add("hk1", "hv1".toByteArray())
                        }
                    }
                )
                send(
                    ProducerRecord(TEST_TOPIC_1, "key_2", "value_2").apply {
                        headers().apply {
                            add("hk", "hv".toByteArray())
                            add("hk2", "hv2".toByteArray())
                        }
                    }
                )
                close()
            }
            val records = broker.query()
                .topic(TEST_TOPIC_1)
                .records()
                .valueDeserializer(RecordDeserializer.string())
            records.anyHeaderKeyMatching { it == "hk1" }.single().value() shouldBe "value_1"
            records.anyHeaderKeyMatching { it == "hk2" }.single().value() shouldBe "value_2"
            records.anyHeaderKeyMatching { it == "hk" }.all().map { it.value() } shouldBe listOf("value_1", "value_2")
        }
    }

    private val createdBrokers = mutableListOf<KafkaTestBroker>()

    override fun afterEach(testCase: TestCase, result: TestResult) {
        createdBrokers.forEach { it.close() }
        createdBrokers.clear()
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
            it["compression.type"] = "none"
        }

    companion object {
        private const val TEST_TOPIC_1 = "test-topic-1"
        private const val TEST_TOPIC_2 = "test-topic-2"
    }
}