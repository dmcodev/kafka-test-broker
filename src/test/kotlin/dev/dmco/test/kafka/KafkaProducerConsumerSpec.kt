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
        props.put("acks", "all")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("compression.type", "none")

        val producer = KafkaProducer<String, String>(props)
        producer.send(ProducerRecord("my-topic", "key", "value")).get()
        producer.close()

        broker.close()
    }
})