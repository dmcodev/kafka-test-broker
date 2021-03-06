package dev.dmcode.test.kafka.usecase.fetch;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.messages.Record;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.response.ResponseHeader;
import dev.dmcode.test.kafka.messages.response.ResponseMessage;
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

    @SinceVersion(1)
    int throttleTimeMs;

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

        int id;

        ErrorCode errorCode;

        long headOffset;

        @SinceVersion(4)
        long lastStableOffset;

        @SinceVersion(4)
        Collection<AbortedTransaction> abortedTransactions;

        @VersionMapping(value = 0, sinceVersion = 0)
        @VersionMapping(value = 2, sinceVersion = 4)
        Collection<Record> records;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class AbortedTransaction {

        long producerId;

        long firstOffset;
    }
}
