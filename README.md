## Kafka Test Broker for the JVM

Lightweight, zero-dependency, in-memory Kafka broker implementation that can be used for integration testing instead of fully fledged embedded Kafka with Zookeeper.

It supports only basic produce / consume API for now, but it should be enough for applications that just write / read Kafka records.

### Requirements

- Java 8 runtime or higher

### Features

- Supports very basic Kafka Consumer & Producer API (Kafka `FindCoordinator`, `OffsetFetch`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `Produce`, `Fetch`, `OffsetCommit` requests). 
- Uses only a single thread in the background to create NIO event loop - can handle multiple concurrent connections using just this one thread.
- No dependencies - implemented in pure Java.


### Usage

#### Creating a broker
The simplest way is just to create a broker with default settings:
```Java
// will listen at localhost:9092 address by default
// every created topic will have one partition, topics are auto-created
TestKafkaBroker broker = new TestKafkaBroker();

// broker is already up and running at this point
```
You can create a broker with custom settings, by constructing `BrokerConfig` first:

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