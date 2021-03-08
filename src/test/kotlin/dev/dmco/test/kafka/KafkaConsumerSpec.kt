package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.core.test.TestResult
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.util.Properties

class KafkaConsumerSpec : StringSpec() {

    init {
        "Should verify consumer groups" {
            val broker = createBroker { KafkaTestBroker() }
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key1", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, "key2", "value2"))
                close()
            }
            val consumer1 = KafkaConsumer<String, String>(clientProperties().also { it["group.id"] = TEST_CONSUMER_GROUP_1 }).apply {
                subscribe(listOf(TEST_TOPIC_1))
                poll(Duration.ofSeconds(1)).also {
                    commitSync()
                }
            }
            val consumer2 = KafkaConsumer<String, String>(clientProperties().also { it["group.id"] = TEST_CONSUMER_GROUP_2 }).apply {
                subscribe(listOf(TEST_TOPIC_1))
                repeat(2) {
                    poll(Duration.ofSeconds(1)).also {
                        commitSync()
                    }
                }
            }
            with(broker.state()) {
                consumerGroups().size shouldBe 2
                consumerGroupsExists(TEST_CONSUMER_GROUP_1) shouldBe true
                consumerGroupsExists(TEST_CONSUMER_GROUP_2) shouldBe true
                consumerGroupsExists(TEST_CONSUMER_GROUP_3) shouldBe false
                with(consumerGroup(TEST_CONSUMER_GROUP_1)) {
                    name() shouldBe TEST_CONSUMER_GROUP_1
                    membersCount() shouldBe 1
                }
                with(consumerGroup(TEST_CONSUMER_GROUP_2)) {
                    name() shouldBe TEST_CONSUMER_GROUP_2
                    membersCount() shouldBe 1
                }
                with(topic(TEST_TOPIC_1).partition(0)) {
                    committedOffset(TEST_CONSUMER_GROUP_1) shouldBe 1
                    committedOffset(TEST_CONSUMER_GROUP_2) shouldBe 2
                    committedOffset(TEST_CONSUMER_GROUP_3) shouldBe null
                }
            }
            consumer1.close()
            consumer2.close()
            with(broker.state()) {
                consumerGroup(TEST_CONSUMER_GROUP_1).membersCount() shouldBe 0
                consumerGroup(TEST_CONSUMER_GROUP_2).membersCount() shouldBe 0
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
            it["heartbeat.interval.ms"] = "100"
            it["enable.auto.commit"] = "false"
            it["max.poll.records"] = "1"
        }

    companion object {
        private const val TEST_TOPIC_1 = "test-topic-1"
        private const val TEST_CONSUMER_GROUP_1 = "test-consumer-group-1"
        private const val TEST_CONSUMER_GROUP_2 = "test-consumer-group-2"
        private const val TEST_CONSUMER_GROUP_3 = "test-consumer-group-3"
    }
}