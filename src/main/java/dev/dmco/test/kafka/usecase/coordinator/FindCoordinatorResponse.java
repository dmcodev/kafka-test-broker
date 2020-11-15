package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

@HeaderVersion(value = 0, sinceApiVersion = 0)
@HeaderVersion(value = 1, sinceApiVersion = 3)
@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class FindCoordinatorResponse implements ResponseMessage {

    ResponseHeader header;

    @ApiVersion(min = 1)
    int throttleTimeMs;

    short errorCode;

    @ApiVersion(min = 1)
    String errorMessage; // TODO: COMPACT_NULLABLE_STRING since 3

    int nodeId;

    @Value(ValueType.STRING)
    String host; // TODO: COMPACT_STRING since 3

    int port;

    @ApiVersion(min = 3)
    @Value(ValueType.TAGS_BUFFER)
    Object tags;
}
