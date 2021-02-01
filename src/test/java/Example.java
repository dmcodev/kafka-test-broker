import dev.dmco.test.kafka.KafkaTestBroker;
import dev.dmco.test.kafka.config.BrokerConfig;
import dev.dmco.test.kafka.config.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Example {

    public static void main(String[] args) {

        // configure and create a broker

        BrokerConfig brokerConfig = BrokerConfig.builder()
            .port(9000)
            .topic(
                TopicConfig.builder()
                    .name("test-topic")
                    .partitionsNumber(4)
                    .build()
            )
            .build();


        KafkaTestBroker broker = new KafkaTestBroker(brokerConfig);

        // broker is up and running, now we can produce and consume records

        Properties clientProperties = new Properties();
        clientProperties.put("bootstrap.servers", brokerConfig.host() + ":" + brokerConfig.port());
        clientProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProperties.put("group.id", "test-consumer-group");
        clientProperties.put("heartbeat.interval.ms", "100");
        clientProperties.put("enable.auto.commit", "false");

        KafkaProducer<String, String> producer = new KafkaProducer<>(clientProperties);
        producer.send(new ProducerRecord<>("test-topic", "first-message-key", "first-message-value"));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProperties);
        consumer.subscribe(Collections.singletonList("test-topic"));

        ConsumerRecords<String, String> firstBatch = consumer.poll(Duration.ofSeconds(5));
        consumer.commitSync();

        producer.send(new ProducerRecord<>("test-topic", "second-message-key", "second-message-value"));

        ConsumerRecords<String, String> secondBatch = consumer.poll(Duration.ofSeconds(5));
        consumer.commitSync();

        // firstBatch == [(first-message-key, first-message-value)]
        // secondBatch == [(second-message-key, second-message-value)]

        producer.close();
        consumer.close();

        // close the broker
        broker.close();
    }
}