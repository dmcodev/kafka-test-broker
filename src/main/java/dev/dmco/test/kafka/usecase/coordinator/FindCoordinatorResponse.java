package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.TypeOverride;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.Optional;

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
    @TypeOverride(value = ValueType.NULLABLE_COMPACT_STRING, sinceApiVersion = 3)
    Optional<String> errorMessage;

    int nodeId;

    @TypeOverride(value = ValueType.COMPACT_STRING, sinceApiVersion = 3)
    String host;

    int port;

    @ApiVersion(min = 3)
    Collection<Tag> tags;
}
