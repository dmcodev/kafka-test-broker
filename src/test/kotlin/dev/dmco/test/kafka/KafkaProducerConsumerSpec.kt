package dev.dmco.test.kafka

import io.kotest.core.spec.style.StringSpec
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties


class KafkaProducerConsumerSpec : StringSpec({

    "Should send message to topic" {
        val broker = TestKafkaBroker()

        val props = Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("compression.type", "gzip")

        val producer = KafkaProducer<String, String>(props)
        val f1 = producer.send(ProducerRecord("my-topic", "key1", "value1"))
        val f2 = producer.send(ProducerRecord("my-topic", "key2", "value2"))

        f1.get()
        f2.get()

        producer.close()

        broker.close()
    }
})