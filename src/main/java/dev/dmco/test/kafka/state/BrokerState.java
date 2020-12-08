package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.TestKafkaBrokerConfig;
import dev.dmco.test.kafka.messages.Record;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
public class BrokerState {

    private final Map<String, Topic> topics = new HashMap<>();
    private final TestKafkaBrokerConfig config;

    public void append(String topicName, int partitionId, Collection<Record> records) {
        topic(topicName).partition(partitionId).append(records);
    }

    private Topic topic(String name) {
        return topics.computeIfAbsent(name, Topic::new);
    }
}
