package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.core.test.TestResult
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

class KafkaProducerSpec : StringSpec() {

    init {
        "Should verify records sent to single partition" {
            val broker = createBroker { KafkaTestBroker() }
            val clientProperties = clientProperties()
            KafkaProducer<String, String>(clientProperties).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key1", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, "key2", "value2"))
                send(ProducerRecord(TEST_TOPIC_2, "key3", "value3"))
                send(ProducerRecord(TEST_TOPIC_2, null, "value4"))
                send(ProducerRecord(TEST_TOPIC_2, "key4", null))
                close()
            }
            with(broker.state()) {
                topicExists(TEST_TOPIC_1) shouldBe true
                topicExists(TEST_TOPIC_2) shouldBe true
                topicExists(TEST_TOPIC_3) shouldBe false
                with(topic(TEST_TOPIC_1)) {
                    name() shouldBe TEST_TOPIC_1
                    partitions().size shouldBe 1
                    partitionExists(0) shouldBe true
                    with(partition(0)) {
                        records().size shouldBe 2
                    }
                }
            }
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
            it["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            it["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            it["compression.type"] = "none"
            it["group.id"] = TEST_CONSUMER_GROUP
            it["heartbeat.interval.ms"] = "100"
            it["enable.auto.commit"] = "false"
        }

    companion object {
        private const val TEST_TOPIC_1 = "test-topic-1"
        private const val TEST_TOPIC_2 = "test-topic-2"
        private const val TEST_TOPIC_3 = "test-topic-3"
        private const val TEST_CONSUMER_GROUP = "test-consumer-group"
    }
}