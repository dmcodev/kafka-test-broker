package dev.dmcode.test.kafka.usecase.coordinator;

import dev.dmcode.test.kafka.messages.Tag;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.response.ResponseHeader;
import dev.dmcode.test.kafka.messages.response.ResponseMessage;
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

    @VersionMapping(value = 0, sinceVersion = 0)
    @VersionMapping(value = 1, sinceVersion = 3)
    ResponseHeader header;

    @SinceVersion(1)
    int throttleTimeMs;

    short errorCode;

    @SinceVersion(1)
    //@TypeOverride(value = ValueType.NULLABLE_COMPACT_STRING, sinceApiVersion = 3)
    Optional<String> errorMessage;

    int nodeId;

    //@TypeOverride(value = ValueType.COMPACT_STRING, sinceApiVersion = 3)
    String host;

    int port;

    @SinceVersion(3)
    Collection<Tag> tags;
}
