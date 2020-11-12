package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.messages.KafkaRequest;
import dev.dmco.test.kafka.messages.RequestMessage;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@KafkaRequest(apiKey = 18)
public class ApiVersionsRequest implements RequestMessage {
    RequestHeader header;
}
