package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.messages.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class MetadataResponse implements ResponseMessage {

}
