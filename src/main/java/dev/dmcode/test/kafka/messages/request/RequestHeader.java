package dev.dmcode.test.kafka.messages.request;

import dev.dmcode.test.kafka.messages.Tag;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
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
