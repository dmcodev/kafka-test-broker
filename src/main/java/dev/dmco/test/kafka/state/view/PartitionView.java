package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.Partition;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@Value
@Builder
@Accessors(fluent = true)
public class PartitionView {

    int index;
    RecordsView records;

    @Getter(AccessLevel.NONE)
    Map<String, Long> committedOffsets;

    public Long committedOffset(String consumerGroup) {
        return committedOffsets.get(consumerGroup);
    }

    static PartitionView from(Partition partition, Collection<ConsumerGroup> consumerGroups) {
        return PartitionView.builder()
            .index(partition.id())
            .records(
                RecordsView.builder()
                    .records(
                        partition.records().values().stream()
                            .map(record -> RecordView.from(record, partition.id()))
                            .collect(Collectors.toList())
                    )
                    .build()
            )
            .committedOffsets(
                consumerGroups.stream()
                    .collect(
                        Collectors.toMap(
                            ConsumerGroup::name,
                            group -> group.offsets().entrySet().stream()
                                .filter(entry -> entry.getKey().equals(partition))
                                .findFirst()
                                .map(Map.Entry::getValue)
                                .orElse(0L)
                        )
                    )
            )
            .build();
    }
}
