package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.state.Partition;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

@Value
@Builder
@Getter(AccessLevel.NONE)
public class PartitionView {

    int index;
    Collection<RecordView> records;

    public int index() {
        return index;
    }

    public Collection<RecordView> records() {
        return new ArrayList<>(records);
    }

    public static PartitionView from(Partition partition) {
        return PartitionView.builder()
            .index(partition.id())
            .records(
                partition.fetch(0, Integer.MAX_VALUE).stream()
                    .map(record -> RecordView.from(record, partition.id()))
                    .collect(Collectors.toList())
            )
            .build();
    }
}
