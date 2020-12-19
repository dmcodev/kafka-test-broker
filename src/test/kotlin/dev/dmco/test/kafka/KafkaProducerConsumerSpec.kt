package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import dev.dmco.test.kafka.config.TopicConfig
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties


class KafkaProducerConsumerSpec : StringSpec({

    "Should send message to topic" {
        val config = BrokerConfig.builder()
            .topic(TopicConfig.create("my-topic", 5))
            .build()
        val broker = TestKafkaBroker(config)

        val props = Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        props.put("compression.type", "gzip")

        props.put("group.id", "my-consumer-group")

        val producer = KafkaProducer<String, String>(props)
        val f1 = producer.send(ProducerRecord("my-topic", "key1", "value1"))
        val f2 = producer.send(ProducerRecord("my-topic", "key2", "value2"))

        f1.get()
        f2.get()
        producer.close()

        Thread.sleep(1000)

        val consumer1 = KafkaConsumer<String, String>(props)
        consumer1.subscribe(mutableListOf("my-topic"))

        val consumer2 = KafkaConsumer<String, String>(props)
        consumer2.subscribe(mutableListOf("my-topic"))

        delay(2000)

        val records1 = consumer1.poll(5000)
        val records2 = consumer2.poll(5000)


        consumer1.close()
        consumer2.close()


        records1.count() shouldBe 1
        broker.close()
    }
})