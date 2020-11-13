package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.messages.meta.Struct;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class ProduceResponse implements ResponseMessage {

    @Struct
    ResponseHeader header;

    int errorCode;
}
