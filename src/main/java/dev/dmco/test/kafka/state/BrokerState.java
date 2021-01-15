package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.config.BrokerConfig;
import dev.dmco.test.kafka.config.TopicConfig;
import dev.dmco.test.kafka.io.EventLoop;
import dev.dmco.test.kafka.logging.Logger;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
@Accessors(fluent = true)
public class BrokerState {

    public static final int NODE_ID = 1;

    private static final Logger LOG = Logger.create(EventLoop.class);

    @Getter
    private final RequestHandlers requestHandlers = new RequestHandlers();

    private final Map<String, Topic> topics = new HashMap<>();
    private final Map<String, ConsumerGroup> consumerGroups = new HashMap<>();

    @Getter
    private final BrokerConfig config;

    public ConsumerGroup getConsumerGroup(String name) {
        return consumerGroups.computeIfAbsent(name, it -> new ConsumerGroup());
    }

    public Topic getTopic(String name) {
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
