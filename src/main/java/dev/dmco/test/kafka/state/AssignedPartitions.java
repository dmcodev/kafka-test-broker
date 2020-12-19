package dev.dmco.test.kafka.state;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Builder
@Accessors(fluent = true)
public class AssignedPartitions {
    String topicName;
    List<Integer> partitionIds;
}
