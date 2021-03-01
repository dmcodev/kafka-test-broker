package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.config.BrokerConfig;
import dev.dmco.test.kafka.config.TopicConfig;
import dev.dmco.test.kafka.logging.Logger;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Value
@RequiredArgsConstructor
@Accessors(fluent = true)
public class BrokerState {

    public static final int NODE_ID = 1;

    private static final Logger LOG = Logger.create(BrokerState.class);

    RequestHandlers requestHandlers = new RequestHandlers();
    Map<String, Topic> topics = new HashMap<>();
    Map<String, ConsumerGroup> consumerGroups = new HashMap<>();

    BrokerConfig config;

    public ConsumerGroup consumerGroup(String name) {
        return consumerGroups.computeIfAbsent(name, ConsumerGroup::new);
    }

    public Topic topic(String name) {
        return topics.computeIfAbsent(name, this::createTopic);
    }

    private Topic createTopic(String name) {
        TopicConfig topicConfig = Optional.ofNullable(config.topics()).orElseGet(Collections::emptyList).stream()
            .filter(it -> name.equals(it.name()))
            .findFirst()
            .orElseGet(() -> TopicConfig.createDefault(name));
        return new Topic(name, topicConfig.partitionsNumber());
    }

    public void reset() {
        topics.clear();
        consumerGroups.clear();
        LOG.debug("Broker state cleared");
    }
}
