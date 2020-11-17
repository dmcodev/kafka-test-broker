package dev.dmco.test.kafka

import io.kotest.core.spec.style.StringSpec
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties


class KafkaProducerConsumerSpec : StringSpec({

    "Should send message to topic" {
        var broker = TestKafkaBroker()

        val props = Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        props.put("compression.type", "none")

        props.put("group.id", "my-consumer-group")

        val producer = KafkaProducer<String, String>(props)
        val f1 = producer.send(ProducerRecord("my-topic", "key1", "value1"))
        val f2 = producer.send(ProducerRecord("my-topic", "key2", "value2"))

        f1.get()
        f2.get()
        producer.close()

        Thread.sleep(1000)

        val consumer = KafkaConsumer<String, String>(props)
        consumer.subscribe(mutableListOf("my-topic"))
        val records = consumer.poll(5000)
        consumer.close()


        broker.close()
    }
})