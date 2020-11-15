package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
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

    @ApiVersion(min = 1)
    @Value(ValueType.NULLABLE_STRING)
    String clientId;

    @ApiVersion(min = 2)
    @Value(ValueType.TAGS_BUFFER)
    byte[] tagsBuffer;
}
