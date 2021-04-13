package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
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
            val broker = createBroker { KafkaTestBroker() }
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

        "Should query topic existence" {
            val broker = createBroker { KafkaTestBroker() }
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key", "value"))
                close()
            }
            broker.query().topic(TEST_TOPIC_1).exists() shouldBe true
            broker.query().topic(TEST_TOPIC_2).exists() shouldBe false
        }
    }

    private val createdBrokers = mutableListOf<KafkaTestBroker>()

    override fun afterEach(testCase: TestCase, result: TestResult) {
        createdBrokers.forEach { it.close() }
        createdBrokers.clear()
    }

    private fun createBroker(supplier: () -> KafkaTestBroker): KafkaTestBroker =
        supplier().also { createdBrokers.add(it) }

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
        private const val TEST_TOPIC_3 = "test-topic-3"
    }
}