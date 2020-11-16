package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import lombok.experimental.Accessors;

import java.util.Collection;

@lombok.Value
@Accessors(fluent = true)
public class RequestHeader {

    short apiKey;
    short apiVersion;
    int correlationId;

    @ApiVersion(min = 1)
    String clientId;

    @ApiVersion(min = 2)
    Collection<Tag> tags;
}
