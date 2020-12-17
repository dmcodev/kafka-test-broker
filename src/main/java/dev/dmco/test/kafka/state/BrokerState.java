package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.TestKafkaBrokerConfig;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
public class BrokerState {

    private final Map<String, Topic> topics = new HashMap<>();
    private final Map<String, ConsumerGroup> consumerGroups = new HashMap<>();

    private final TestKafkaBrokerConfig config;

    public ConsumerGroup consumerGroup(String name) {
        return consumerGroups.computeIfAbsent(name, ConsumerGroup::new);
    }

    public Topic topic(String name) {
        return topics.computeIfAbsent(name, Topic::new);
    }
}
