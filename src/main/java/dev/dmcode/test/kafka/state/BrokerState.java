package dev.dmcode.test.kafka.state;

import dev.dmcode.test.kafka.config.BrokerConfig;
import dev.dmcode.test.kafka.config.TopicConfig;
import dev.dmcode.test.kafka.logging.Logger;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Accessors(fluent = true)
public class BrokerState {

    public static final int NODE_ID = 1;

    private static final Logger LOG = Logger.create(BrokerState.class);

    @Getter private final RequestHandlers requestHandlers = new RequestHandlers();
    private final Map<String, Topic> topics = new HashMap<>();
    private final Map<String, ConsumerGroup> consumerGroups = new HashMap<>();

    @Getter private final BrokerConfig config;

    public BrokerState(BrokerConfig config) {
        this.config = config;
    }

    public ConsumerGroup getOrCreateConsumerGroup(String name) {
        return consumerGroups.computeIfAbsent(name, ConsumerGroup::new);
    }

    public Topic getOrCreateTopic(String name) {
        return topics.computeIfAbsent(name, this::createTopic);
    }

    public Optional<Topic> getTopic(String name) {
        return Optional.ofNullable(topics.get(name));
    }

    public void reset() {
        topics.clear();
        consumerGroups.clear();
        LOG.debug("Broker state cleared");
    }

    private Topic createTopic(String name) {
        TopicConfig topicConfig = Optional.ofNullable(config.topics()).orElseGet(Collections::emptyList).stream()
            .filter(it -> name.equals(it.name()))
            .findFirst()
            .orElseGet(() -> TopicConfig.createDefault(name));
        return new Topic(name, topicConfig.partitionsNumber());
    }
}
