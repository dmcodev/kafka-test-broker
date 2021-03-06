package dev.dmcode.test.kafka.messages.consumer;

import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Builder
@lombok.Value
@AllArgsConstructor
@Accessors(fluent = true)
public class Subscription implements ConsumerMessage {

    short version;

    Collection<String> topics;

    Optional<byte[]> userData;

    @SinceVersion(1)
    List<PartitionAssignments> partitionAssignments;

    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class PartitionAssignments {

        String topicName;

        List<Integer> partitionIds;
    }
}
