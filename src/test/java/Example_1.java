import dev.dmcode.test.kafka.KafkaTestBroker;
import dev.dmcode.test.kafka.config.BrokerConfig;
import dev.dmcode.test.kafka.config.TopicConfig;
import dev.dmcode.test.kafka.state.query.RecordSetQuery;
import dev.dmcode.test.kafka.state.query.deserializer.RecordDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class Example_1 {

    private static final String TOPIC_NAME = "test-topic";

    public static void main(String[] args) {
        // create broker config and client properties
        BrokerConfig brokerConfig = createBrokerConfig();
        Properties clientProperties = createClientProperties(brokerConfig);
        // create Kafka test broker
        try (KafkaTestBroker broker = new KafkaTestBroker(brokerConfig)) {
            // send some messages using producer
            sendMessage(clientProperties);
            // verify sent messages using consumer
            ConsumerRecords<String, String> receivedMessages = receiveMessages(clientProperties);
            verifyReceivedMessages(receivedMessages);
            // verify sent messages by using broker query API
            verifyReceivedMessages(broker);
        }
    }

    private static void sendMessage(Properties clientProperties) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(clientProperties)) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key_1", "value_1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key_2", "value_2"));
        }
    }

    private static ConsumerRecords<String, String> receiveMessages(Properties clientProperties) {
        ConsumerRecords<String, String> records;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProperties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            records = consumer.poll(Duration.ofSeconds(5));
            consumer.commitSync();
        }
        return records;
    }

    private static void verifyReceivedMessages(ConsumerRecords<String, String> records) {
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        assert records.count() == 2;
        assert "value_1".equals(iterator.next().value());
        assert "value_2".equals(iterator.next().value());
    }

    private static void verifyReceivedMessages(KafkaTestBroker broker) {
        RecordSetQuery<String, String, byte[]> query = broker.query()
            .topic(TOPIC_NAME)
            .records()
            .keyValueDeserializer(RecordDeserializer.string());
        assert query.keyMatching("key_1"::equals).single().value().equals("value_1");
        assert query.keyMatching(key -> key.startsWith("key")).all().size() == 2;
        assert query.valueMatching(value -> value.startsWith("value")).all().size() == 2;
    }

    private static BrokerConfig createBrokerConfig() {
        TopicConfig testTopicConfig = TopicConfig.builder()
            .name(TOPIC_NAME)
            .partitionsNumber(4)
            .build();
        return BrokerConfig.builder()
            .port(9000)
            .topic(testTopicConfig)
            .build();
    }

    private static Properties createClientProperties(BrokerConfig brokerConfig) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerConfig.host() + ":" + brokerConfig.port());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-consumer-group");
        properties.put("heartbeat.interval.ms", "100");
        properties.put("enable.auto.commit", "false");
        return properties;
    }
}