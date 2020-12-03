package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.metadata.ApiVersionMapping;
import dev.dmco.test.kafka.messages.metadata.SinceApiVersion;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.Optional;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class FindCoordinatorResponse implements ResponseMessage {

    @ApiVersionMapping(value = 0, sinceApiVersion = 0)
    @ApiVersionMapping(value = 1, sinceApiVersion = 3)
    ResponseHeader header;

    @SinceApiVersion(1)
    int throttleTimeMs;

    short errorCode;

    @SinceApiVersion(1)
    //@TypeOverride(value = ValueType.NULLABLE_COMPACT_STRING, sinceApiVersion = 3)
    Optional<String> errorMessage;

    int nodeId;

    //@TypeOverride(value = ValueType.COMPACT_STRING, sinceApiVersion = 3)
    String host;

    int port;

    @SinceApiVersion(3)
    Collection<Tag> tags;
}
