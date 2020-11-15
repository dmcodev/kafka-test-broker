package dev.dmco.test.kafka.messages.common;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.Value;
import lombok.experimental.Accessors;

@lombok.Value
@Accessors(fluent = true)
public class Record {

    @Value(ValueType.VARINT)
    Integer length;
}
