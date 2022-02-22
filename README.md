### Kafka Test Broker for the JVM

Lightweight, zero-dependency, in-memory Kafka broker implementation. It implements a subset of the Kafka protocol that allows for message production and consumption. This is an educational project.

#### Requirements

- Java 8 runtime or higher

#### Features

- Supports basic Kafka Consumer & Producer API (Kafka `FindCoordinator`, `OffsetFetch`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `Produce`, `Fetch`, `OffsetCommit` requests). 
- Uses only a single background thread create event loop.
- Uses non-blocking IO to handle multiple concurrent connections
- No third-party dependencies - implemented in pure Java.
- Provides a convenient broker query API to verify sent messages without creating a Kafka consumer.

#### Usage

Please see this [example](src/test/java/Example_1.java).
