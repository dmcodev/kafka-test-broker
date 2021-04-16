## Kafka Test Broker for the JVM

Lightweight, zero-dependency, in-memory Kafka broker implementation designed for integration testing.

It supports only basic Produce and Consume API, but that should be enough for most of the applications that just write and read Kafka records.

### Requirements

- Java 8 runtime or higher

### Features

- Supports basic Kafka Consumer & Producer API (Kafka `FindCoordinator`, `OffsetFetch`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `Produce`, `Fetch`, `OffsetCommit` requests). 
- Uses only a single thread in the background to create NIO event loop - can handle multiple concurrent connections using just this one thread.
- No dependencies - implemented in pure Java.

### Usage

#### Creating a broker
Creating a broker with default settings:
```Java
// will listen at localhost:9092 address by default
// every topic will have one partition, topics are auto-created
TestKafkaBroker broker = new TestKafkaBroker();

// broker is already up and running at this point
```
Creating a broker with custom settings:

```Java
BrokerConfig config = BrokerConfig.builder()
    .port(9000) // change the default port
    .topic(
        // topic "test-topic" will have 10 partitions 
        TopicConfig.builder()
            .name("test-topic")
            .partitionsNumber(10)
            .build()
    )
    .build();

TestKafkaBroker broker = new TestKafkaBroker(config);

// Kafka producers / consumers can connect to localhost:9000 now
```
#### Resetting the state of a broker
```Java
// deletes all topic data / partitions
// resets all consumer groups and committed offsets
// disconnects all clients
broker.reset();
```

#### Closing a broker
`TestKafkaBroker` implements `AutoCloseable` for convenience.
```Java
// release event loop thread and close all opened sockets
broker.close();
```

#### Complete example with producer and consumer
```Java
import KafkaTestBroker;
import BrokerConfig;
import TopicConfig;
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
```