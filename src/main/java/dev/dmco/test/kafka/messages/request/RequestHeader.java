package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.RequiredApiVersion;
import dev.dmco.test.kafka.messages.meta.Value;
import lombok.experimental.Accessors;

@lombok.Value
@Accessors(fluent = true)
public class RequestHeader {

    @Value(ValueType.INT16)
    Short apiKey;

    @Value(ValueType.INT16)
    Short apiVersion;

    @Value(ValueType.INT32)
    Integer correlationId;

    @RequiredApiVersion(min = 1)
    @Value(ValueType.NULLABLE_STRING)
    String clientId;

    @RequiredApiVersion(min = 2)
    @Value(ValueType.TAGS_BUFFER)
    byte[] tagsBuffer;
}
