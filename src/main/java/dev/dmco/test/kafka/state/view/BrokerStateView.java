package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.state.BrokerState;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Immutable snapshot of broker's state.
 */
@Value
@Builder
@Getter(AccessLevel.NONE)
public class BrokerStateView {

    Map<String, TopicView> topics;
    Map<String, ConsumerGroupView> consumerGroups;

    /**
     * @return All created topics.
     */
    public Collection<TopicView> topics() {
        return topics.values();
    }

    /**
     * @param name Name of the topic.
     * @return True if topic with given name exists.
     */
    public boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    /**
     * @param name Name of the topic.
     * @return Topic with given name.
     * @throws IllegalArgumentException When topic with given name does not exist.
     */
    public TopicView topic(String name) {
        return Optional.ofNullable(topics.get(name))
            .orElseThrow(() -> new IllegalArgumentException("Topic " + name + " does not exist"));
    }

    /**
     * @return All consumer groups.
     */
    public Collection<ConsumerGroupView> consumerGroups() {
        return consumerGroups.values();
    }

    /**
     * @param name Name of the consumer group.
     * @return True if consumer group with given name exists.
     */
    public boolean consumerGroupsExists(String name) {
        return consumerGroups.containsKey(name);
    }

    /**
     * @param name Name of the consumer group.
     * @return Consumer group with given name.
     * @throws IllegalArgumentException When consumer group with given name does not exist.
     */
    public ConsumerGroupView consumerGroup(String name) {
        return Optional.ofNullable(consumerGroups.get(name))
            .orElseThrow(() -> new IllegalArgumentException("Consumer group " + name + " does not exist"));
    }

    public static BrokerStateView from(BrokerState state) {
        return BrokerStateView.builder()
            .topics(
                state.topics().values().stream()
                    .map(topic -> TopicView.from(topic, state.consumerGroups().values()))
                    .collect(Collectors.toMap(TopicView::name, Function.identity()))
            )
            .consumerGroups(
                state.consumerGroups().values().stream()
                    .map(ConsumerGroupView::from)
                    .collect(Collectors.toMap(ConsumerGroupView::name, Function.identity()))
            )
            .build();
    }
}
