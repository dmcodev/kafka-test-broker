package dev.dmco.test.kafka.usecase.offsetfetch;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class OffsetFetchResponse implements ResponseMessage {

    @VersionMapping(value = 0, sinceVersion = 0)
    @VersionMapping(value = 1, sinceVersion = 6)
    ResponseHeader header;

    @Singular
    List<Topic> topics;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class Topic {

        String name;

        @Singular
        List<Partition> partitions;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class Partition {

        int partitionId;

        long committedOffset;

        Optional<String> metadata;

        ErrorCode errorCode;
    }
}
