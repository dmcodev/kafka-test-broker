package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class ProduceResponse implements ResponseMessage {

    @ApiVersionOverride(value = 0, sinceVersion = 0)
    ResponseHeader header;
}
