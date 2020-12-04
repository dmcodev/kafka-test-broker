package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.Optional;

@lombok.Value
@Accessors(fluent = true)
public class RequestHeader {

    short apiKey;

    short apiVersion;

    int correlationId;

    @SinceVersion(1)
    Optional<String> clientId;

    @SinceVersion(2)
    Collection<Tag> tags;
}
