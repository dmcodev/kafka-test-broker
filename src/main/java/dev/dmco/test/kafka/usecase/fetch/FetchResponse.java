package dev.dmco.test.kafka.usecase.fetch;

import dev.dmco.test.kafka.messages.Record;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.List;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class FetchResponse implements ResponseMessage {

    @VersionMapping(value = 0, sinceVersion = 0)
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

        short errorCode;

        long headOffset;

        Collection<Record> records;
    }
}
