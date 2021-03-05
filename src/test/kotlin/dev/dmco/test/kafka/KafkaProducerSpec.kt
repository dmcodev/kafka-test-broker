package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import dev.dmco.test.kafka.config.TopicConfig
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.core.test.TestResult
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldStartWith
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.nio.charset.StandardCharsets
import java.util.Properties

class KafkaProducerSpec : StringSpec() {

    init {
        "Should verify records sent to single partition" {
            val broker = createBroker { KafkaTestBroker() }
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, "key1", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, "key2", "value2"))
                send(ProducerRecord(TEST_TOPIC_2, "key3", "value3"))
                send(ProducerRecord(TEST_TOPIC_2, null, "value4"))
                send(ProducerRecord(TEST_TOPIC_2, "key4", null))
                close()
            }
            with(broker.state()) {
                topics().size shouldBe 2
                topicExists(TEST_TOPIC_1) shouldBe true
                topicExists(TEST_TOPIC_2) shouldBe true
                topicExists(TEST_TOPIC_3) shouldBe false
                with(topic(TEST_TOPIC_1)) {
                    name() shouldBe TEST_TOPIC_1
                    partitions().size shouldBe 1
                    partitionExists(0) shouldBe true
                    partitionExists(1) shouldBe false
                    with(partition(0)) {
                        index() shouldBe 0
                        with(records()) {
                            with(all().toList()) {
                                size shouldBe 2
                                with(get(0)) {
                                    key() shouldBe "key1".toByteArray()
                                    key { it.size } shouldBe 4
                                    keyString() shouldBe "key1"
                                    keyString(StandardCharsets.UTF_16) shouldBe String("key1".toByteArray(), StandardCharsets.UTF_16)
                                    value() shouldBe "value1".toByteArray()
                                    value { it.size } shouldBe 6
                                    valueString() shouldBe "value1"
                                    valueString(StandardCharsets.UTF_16) shouldBe String("value1".toByteArray(), StandardCharsets.UTF_16)
                                    header("") shouldBe null
                                    headerString("") shouldBe null
                                    headerString("", StandardCharsets.UTF_16BE) shouldBe null
                                    headers().isEmpty() shouldBe true
                                    headers { it.size }.isEmpty() shouldBe true
                                }
                                with(get(1)) {
                                    key() shouldBe "key2".toByteArray()
                                    keyString() shouldBe "key2"
                                    keyString(StandardCharsets.UTF_16) shouldBe String("key2".toByteArray(), StandardCharsets.UTF_16)
                                    value() shouldBe "value2".toByteArray()
                                    valueString() shouldBe "value2"
                                    valueString(StandardCharsets.UTF_16) shouldBe String("value2".toByteArray(), StandardCharsets.UTF_16)
                                    headers().isEmpty() shouldBe true
                                }
                            }
                            firstByKey("key1").valueString() shouldBe "value1"
                            firstByKey("key1".toByteArray()).valueString() shouldBe "value1"
                            firstByKey("key2").valueString() shouldBe "value2"
                            firstByKey("key3") shouldBe null
                            firstByValue("value1").keyString() shouldBe "key1"
                            firstByValue("value1".toByteArray()).keyString() shouldBe "key1"
                            firstByValue("value2").keyString() shouldBe "key2"
                            firstByValue("value3") shouldBe null
                            allByKey("key1").first().valueString() shouldBe "value1"
                            allByKey("key1".toByteArray()).first().valueString() shouldBe "value1"
                            allByKey("key3").isEmpty() shouldBe true
                            allByValue("value1").first().keyString() shouldBe "key1"
                            allByValue("value1".toByteArray()).first().keyString() shouldBe "key1"
                            allByValue("value3").isEmpty() shouldBe true
                            first { it.keyString().endsWith("2") }.valueString() shouldBe "value2"
                            first { it.keyString().endsWith("3") } shouldBe null
                            all { it.keyString().endsWith("2") }.size shouldBe 1
                            all { it.keyString().endsWith("3") }.isEmpty() shouldBe true
                        }
                    }
                }
                with(topic(TEST_TOPIC_2)) {
                    name() shouldBe TEST_TOPIC_2
                    partitions().size shouldBe 1
                    partitionExists(0) shouldBe true
                    partitionExists(1) shouldBe false
                    with(partition(0)) {
                        index() shouldBe 0
                        with(records()) {
                            with(all().toList()) {
                                size shouldBe 3
                                with(get(0)) {
                                    keyString() shouldBe "key3"
                                    valueString() shouldBe "value3"
                                }
                                with(get(1)) {
                                    key() shouldBe null
                                    keyString() shouldBe null
                                    keyString(StandardCharsets.UTF_16) shouldBe null
                                    valueString() shouldBe "value4"
                                }
                                with(get(2)) {
                                    keyString() shouldBe "key4"
                                    value() shouldBe null
                                    valueString() shouldBe null
                                    valueString(StandardCharsets.UTF_16) shouldBe null
                                }
                            }
                        }
                    }
                }
            }
        }

        "Should verify records with headers" {
            val broker = createBroker { KafkaTestBroker() }
            KafkaProducer<String, String>(clientProperties()).apply {
                val record = ProducerRecord(TEST_TOPIC_1, "key", "value", )
                    .apply { headers().add("h", "abc".toByteArray()) }
                send(record)
                close()
            }
            val record = broker.state()
                .topic(TEST_TOPIC_1)
                .partition(0)
                .records()
                .firstByKey("key")
            with(record) {
                containsHeader("h") shouldBe true
                containsHeader("x") shouldBe false
                headers().size shouldBe 1
                header("h") shouldBe "abc".toByteArray()
                header("x") shouldBe null
                header("h") { it.size } shouldBe 3
                headerString("h") shouldBe "abc"
                headerString("x") shouldBe null
            }
        }

        "Should verify records aggregated from multiple partitions" {
            val broker = createBroker {
                val config = BrokerConfig.builder()
                    .topic(
                        TopicConfig.builder()
                            .name(TEST_TOPIC_1)
                            .partitionsNumber(2)
                            .build()
                    )
                    .build()
                KafkaTestBroker(config)
            }
            KafkaProducer<String, String>(clientProperties()).apply {
                send(ProducerRecord(TEST_TOPIC_1, 0, "key", "value1"))
                send(ProducerRecord(TEST_TOPIC_1, 1, "key", "value2"))
                close()
            }
            with(broker.state().topic(TEST_TOPIC_1)) {
                partition(0).records().all().size shouldBe 1
                partition(1).records().all().size shouldBe 1
                with(records()) {
                    all().size shouldBe 2
                    firstByKey("key").valueString() shouldStartWith "value"
                    allByKey("key").size shouldBe 2
                    firstByValue("value2").partitionIndex() shouldBe 1
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
            it["compression.type"] = "none"
        }

    companion object {
        private const val TEST_TOPIC_1 = "test-topic-1"
        private const val TEST_TOPIC_2 = "test-topic-2"
        private const val TEST_TOPIC_3 = "test-topic-3"
    }
}