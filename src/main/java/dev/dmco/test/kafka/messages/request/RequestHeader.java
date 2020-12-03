package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.meta.SinceApiVersion;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.Optional;

@lombok.Value
@Accessors(fluent = true)
public class RequestHeader {

    short apiKey;

    short apiVersion;

    int correlationId;

    @SinceApiVersion(1)
    Optional<String> clientId;

    @SinceApiVersion(2)
    Collection<Tag> tags;
}
