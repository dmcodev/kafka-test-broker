package dev.dmco.test.kafka.messages.request;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class RequestHeader {
    short apiKey;
    short apiVersion;
    short correlationId;
}
