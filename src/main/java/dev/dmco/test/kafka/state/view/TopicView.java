package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.Topic;
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
public class TopicView {

    String name;
    Map<Integer, PartitionView> partitions;

    public String name() {
        return name;
    }

    public Collection<PartitionView> partitions() {
        return partitions.values();
    }

    public boolean partitionExists(int index) {
        return partitions.containsKey(index);
    }

    public PartitionView partition(int index) {
        return Optional.ofNullable(partitions.get(index))
            .orElseThrow(() -> new IllegalArgumentException("Partition with index " + index + " does not exist"));
    }

    public RecordsView records() {
        return partitions.values().stream()
            .map(PartitionView::records)
            .reduce(RecordsView::merge)
            .orElseGet(RecordsView::empty);
    }

    static TopicView from(Topic state, Collection<ConsumerGroup> consumerGroups) {
        return TopicView.builder()
            .name(state.name())
            .partitions(
                state.partitions().values().stream()
                    .map(partition -> PartitionView.from(partition, consumerGroups))
                    .collect(Collectors.toMap(PartitionView::index, Function.identity()))
            )
            .build();
    }
}
