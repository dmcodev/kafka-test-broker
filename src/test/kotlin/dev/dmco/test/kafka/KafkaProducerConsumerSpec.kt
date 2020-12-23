package dev.dmco.test.kafka

import dev.dmco.test.kafka.config.BrokerConfig
import dev.dmco.test.kafka.config.TopicConfig
import io.kotest.core.spec.style.StringSpec
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
        props.put("heartbeat.interval.ms", "50")

        val producer = KafkaProducer<String, String>(props)

        producer.send(ProducerRecord("my-topic", "key1", "value1")).get()
        producer.send(ProducerRecord("my-topic", "key2", "value2")).get()

        //producer.close()

        Thread.sleep(1000)


        val consumer1 = KafkaConsumer<String, String>(props)
        consumer1.subscribe(mutableListOf("my-topic"))

        val consumer2 = KafkaConsumer<String, String>(props)
        consumer2.subscribe(mutableListOf("my-topic"))

        val c1 = GlobalScope.launch {
            while(true) {
                val records = consumer1.poll(2000)
                if (records.count() > 0) {
                    println("[1] Fetched records: ${records.count()}")
                }
            }
        }

        delay(1000)

        val c2 = GlobalScope.launch {
            while(true) {
                val records = consumer2.poll(2000)
                if (records.count() > 0) {
                    println("[2] Fetched records: ${records.count()}")
                }

            }
        }

        val p1 = GlobalScope.launch {
            while(true) {
                repeat(10) {
                    producer.send(ProducerRecord("my-topic", "key$it", "value$it")).get()
                }
                delay(1000)
            }
        }



        delay(100000)

        p1.cancelAndJoin()
        producer.close()

        c1.cancelAndJoin()
        c2.cancelAndJoin()

        consumer1.close()
        consumer2.close()

//        consumer1.close()
//        consumer2.close()


        //records1.count() shouldBe 1
        broker.close()
    }
})