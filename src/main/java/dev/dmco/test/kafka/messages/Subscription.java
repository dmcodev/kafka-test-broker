package dev.dmco.test.kafka.messages;

import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

@Builder
@lombok.Value
@AllArgsConstructor
@Accessors(fluent = true)
public class Subscription {

    short version;

    List<String> topics;

    Optional<byte[]> userData;

    @SinceVersion(1)
    @Singular
    List<PartitionAssignments> partitionAssignments;

    @Builder
    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class PartitionAssignments {

        String topicName;

        List<Integer> partitions;
    }
}
