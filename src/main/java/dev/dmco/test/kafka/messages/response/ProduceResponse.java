package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;

import java.util.List;

@lombok.Value
@Builder
@AllArgsConstructor
public class ProduceResponse implements ResponseMessage {

    @ApiVersionOverride(value = 0, sinceVersion = 0)
    ResponseHeader header;

    @StructSequence(Topic.class)
    @Singular
    List<Topic> topics;

    @ApiVersion(min = 1)
    @Value(ValueType.INT32)
    Integer throttleTimeMs;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class Topic {

        @Value(ValueType.STRING)
        String name;

        @StructSequence(Partition.class)
        @Singular
        List<Partition> partitions;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class Partition {

        @Value(ValueType.INT32)
        Integer partition;

        @Value(ValueType.INT16)
        Short errorCode;

        @Value(ValueType.INT64)
        Long baseOffset;

        @ApiVersion(min = 2)
        @Value(ValueType.INT64)
        Long logAppendTime;
    }
}
