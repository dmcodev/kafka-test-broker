package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.Collection;

@Request(apiKey = 10)
@ApiVersion(max = 2)
@HeaderVersion(value = 1, sinceApiVersion = 0)
@HeaderVersion(value = 2, sinceApiVersion = 3)
@lombok.Value
@Accessors(fluent = true)
public class FindCoordinatorRequest implements RequestMessage {

    RequestHeader header;

    String key;

    @ApiVersion(min = 1)
    byte keyType;

    @ApiVersion(min = 3)
    Collection<Tag> tags;
}
