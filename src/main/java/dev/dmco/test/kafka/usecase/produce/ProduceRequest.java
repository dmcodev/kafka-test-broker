package dev.dmco.test.kafka.usecase.produce;

import dev.dmco.test.kafka.messages.metadata.ApiVersion;
import dev.dmco.test.kafka.messages.metadata.ApiVersionMapping;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.SinceApiVersion;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

@Request(apiKey = 0)
@ApiVersion(max = 2)
@lombok.Value
@Accessors(fluent = true)
public class ProduceRequest implements RequestMessage {

    @ApiVersionMapping(value = 1, sinceApiVersion = 0)
    RequestHeader header;

    @SinceApiVersion(3)
    Optional<String> transactionalId;

    short acknowledgments;

    int timeout;

    List<TopicData> topicData;

    @lombok.Value
    @Accessors(fluent = true)
    public static class TopicData {

        String topic;

        List<PartitionRecordSet> data;
    }

    @lombok.Value
    @Accessors(fluent = true)
    public static class PartitionRecordSet {

        int partition;

        List<Record> records;
    }

    @lombok.Value
    @Builder
    @Accessors(fluent = true)
    public static class Record {

        byte[] key;

        byte[] value;
    }
}
