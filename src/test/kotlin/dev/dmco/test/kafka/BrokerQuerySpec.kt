package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import dev.dmco.test.kafka.config.TopicConfig
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.core.test.TestResult
import io.kotest.matchers.shouldBe
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
            val query = broker.query().topic(TEST_TOPIC_1)
            query.exists() shouldBe false
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value"))
                close()
            }
            query.exists() shouldBe true
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