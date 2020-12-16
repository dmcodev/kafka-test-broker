package dev.dmco.test.kafka.messages.consumer;

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
public class Assignment implements ConsumerMessage {

    short version;

    @Singular
    List<PartitionAssignments> partitionAssignments;

    Optional<byte[]> userData;

    @Builder
    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class PartitionAssignments {

        String topicName;

        List<Integer> partitions;
    }
}
