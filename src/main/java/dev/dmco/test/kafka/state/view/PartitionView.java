package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.state.Partition;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.stream.Collectors;

@Value
@Builder
@Accessors(fluent = true)
public class PartitionView {

    int index;
    RecordsView records;

    public static PartitionView from(Partition partition) {
        return PartitionView.builder()
            .index(partition.id())
            .records(
                RecordsView.builder()
                    .records(
                        partition.fetch(0, Integer.MAX_VALUE).stream()
                            .map(record -> RecordView.from(record, partition.id()))
                            .collect(Collectors.toList())
                    )
                    .build()
            )
            .build();
    }
}
