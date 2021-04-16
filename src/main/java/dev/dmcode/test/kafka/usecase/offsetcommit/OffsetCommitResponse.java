package dev.dmcode.test.kafka.usecase.offsetcommit;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.response.ResponseHeader;
import dev.dmcode.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class OffsetCommitResponse implements ResponseMessage {

    @VersionMapping(value = 0, sinceVersion = 0)
    @VersionMapping(value = 1, sinceVersion = 8)
    ResponseHeader header;

    List<Topic> topics;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class Topic {

        String name;

        List<Partition> partitions;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class Partition {

        int id;

        ErrorCode errorCode;
    }
}
