package dev.dmco.test.kafka.state;

import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Value
@With
@Builder
@Accessors(fluent = true)
public class Member {

    static final String NAME_PREFIX = "member";

    String id;
    Set<String> protocols;
    List<AssignedPartitions> partitionAssignments;

    public List<String> subscribedTopics() {
        return partitionAssignments.stream()
            .map(AssignedPartitions::topicName)
            .collect(Collectors.toList());
    }
}
