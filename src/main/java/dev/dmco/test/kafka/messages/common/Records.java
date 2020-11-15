package dev.dmco.test.kafka.messages.common;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Builder
@Accessors(fluent = true)
public class Records {

    @Value(ValueType.INT64)
    Long baseOffset;

    @Value(ValueType.INT32)
    Integer batchLength;

    @Value(ValueType.INT32)
    Integer partitionLeaderEpoch;

    @Value(ValueType.INT8)
    Byte magic;

    @Value(ValueType.INT32)
    Integer crc;

    @Value(ValueType.INT16)
    Short attributes;

    @Value(ValueType.INT32)
    Integer lastOffsetDelta;

    @Value(ValueType.INT64)
    Long firstTimestamp;

    @Value(ValueType.INT64)
    Long maxTimestamp;

    @Value(ValueType.INT64)
    Long producerId;

    @Value(ValueType.INT16)
    Short producerEpoch;

    @Value(ValueType.INT32)
    Integer baseSequence;

    @StructSequence(Record.class)
    List<Record> records;
}
