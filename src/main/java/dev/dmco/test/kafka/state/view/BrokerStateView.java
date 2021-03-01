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

@Value
@Builder
@Getter(AccessLevel.NONE)
public class BrokerStateView {

    Map<String, TopicView> topics;
    Map<String, ConsumerGroupView> consumerGroups;

    public Collection<TopicView> topics() {
        return topics.values();
    }

    public boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    public TopicView topic(String name) {
        return Optional.ofNullable(topics.get(name))
            .orElseThrow(() -> new IllegalArgumentException("Topic " + name + " does not exist"));
    }

    public Collection<ConsumerGroupView> consumerGroups() {
        return consumerGroups.values();
    }

    public boolean consumerGroupsExists(String name) {
        return consumerGroups.containsKey(name);
    }

    public ConsumerGroupView consumerGroup(String name) {
        return Optional.ofNullable(consumerGroups.get(name))
            .orElseThrow(() -> new IllegalArgumentException("Consumer group " + name + " does not exist"));
    }

    public static BrokerStateView from(BrokerState state) {
        return BrokerStateView.builder()
            .topics(
                state.topics().values().stream()
                    .map(TopicView::from)
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
