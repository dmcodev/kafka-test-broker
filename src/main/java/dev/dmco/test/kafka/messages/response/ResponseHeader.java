package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.Value;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.Accessors;

@lombok.Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class ResponseHeader {

    @Value(ValueType.INT32)
    Integer correlationId;

    @ApiVersion(min = 1)
    @Value(ValueType.TAGS_BUFFER)
    byte[] tagsBuffer;
}
