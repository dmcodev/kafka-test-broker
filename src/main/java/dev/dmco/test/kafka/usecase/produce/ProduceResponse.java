package dev.dmco.test.kafka.usecase.produce;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;

import java.util.List;

@HeaderVersion(value = 0, sinceApiVersion = 0)
@lombok.Value
@Builder
@AllArgsConstructor
public class ProduceResponse implements ResponseMessage {

    @Value(ValueType.RESPOSNE_HEADER)
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
