package dev.dmco.test.kafka.messages;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class RequestHeader {
    short apiKey;
    short apiVersion;
    int correlationId;
}
