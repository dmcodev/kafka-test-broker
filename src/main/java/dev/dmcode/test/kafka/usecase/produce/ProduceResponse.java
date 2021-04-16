package dev.dmcode.test.kafka.usecase.produce;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.response.ResponseHeader;
import dev.dmcode.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class ProduceResponse implements ResponseMessage {

    @VersionMapping(value = 0, sinceVersion = 0)
    ResponseHeader header;

    @Singular
    List<Topic> topics;

    @SinceVersion(1)
    int throttleTimeMs;

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

        int id;

        ErrorCode errorCode;

        long baseOffset;

        @SinceVersion(2)
        long appendTime;
    }
}
