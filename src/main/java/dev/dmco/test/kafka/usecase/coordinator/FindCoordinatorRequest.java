package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.metadata.ApiVersion;
import dev.dmco.test.kafka.messages.metadata.ApiVersionMapping;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.SinceApiVersion;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.Collection;

@Request(apiKey = 10)
@ApiVersion(max = 2)
@lombok.Value
@Accessors(fluent = true)
public class FindCoordinatorRequest implements RequestMessage {

    @ApiVersionMapping(value = 1, sinceApiVersion = 0)
    @ApiVersionMapping(value = 2, sinceApiVersion = 3)
    RequestHeader header;

    // @TypeOverride(value = ValueType.COMPACT_STRING, sinceApiVersion = 3)
    String key;

    @SinceApiVersion(1)
    byte keyType;

    @SinceApiVersion(3)
    Collection<Tag> tags;
}
