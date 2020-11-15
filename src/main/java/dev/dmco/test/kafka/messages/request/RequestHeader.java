package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.Value;
import lombok.experimental.Accessors;

@lombok.Value
@Accessors(fluent = true)
public class RequestHeader {

    short apiKey;
    short apiVersion;
    int correlationId;

    @ApiVersion(min = 1)
    String clientId;

    @ApiVersion(min = 2)
    @Value(ValueType.TAGS_BUFFER)
    byte[] tagsBuffer;
}
