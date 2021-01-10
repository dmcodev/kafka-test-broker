package dev.dmco.test.kafka.state;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Subscriptions {

    private final Map<String, Subscription> subscriptions = new HashMap<>();

    public void createIfAbsent(String topicName) {
        subscriptions.computeIfAbsent(topicName, it -> new Subscription());
    }

    public Subscription get(String topicName) {
        return subscriptions.computeIfAbsent(topicName, key -> new Subscription());
    }

    public Collection<String> topicNames() {
        return subscriptions.keySet();
    }

    public Collection<Partition> assignedPartitions() {
        return subscriptions.values().stream()
            .map(Subscription::getPartitions)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }
}
