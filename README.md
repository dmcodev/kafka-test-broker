## Kafka Test Broker for the JVM

This is a work in progress...

This project is a result of me studying Kafka and its protocol. I wanted to write a zero-dependency mini Kafka broker in Java that would support basic consume / produce API.

In the future I'd like to be able to use this broker as a replacement for embedded Kafka in integration tests that are using simple consume / produce API.

### Usage

```Java
// create configuration
BrokerConfig config = BrokerConfig.builder()
    .host("localhost")
    .port(9092)
    .topic(
        // if not specified, by default 1 partition will be created for a topic
        TopicConfig.builder()
            .name("test-topic")
            .partitionsNumber(10)
            .build()
    )
    .build();

TestKafkaBroker broker = new TestKafkaBroker(config);

// connect to broker and consume / produce records

// in any time you can reset the state of the broker instead of closing and recreating it
broker.reset();

// broker should be closed at the end to release event loop thread and to close all opened sockets
broker.close();

```