package dev.dmco.test.kafka.usecase.heartbeat;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class HeartBeatResponse implements ResponseMessage {

    @VersionMapping(value = 0, sinceVersion = 0)
    @VersionMapping(value = 1, sinceVersion = 4)
    ResponseHeader header;

    ErrorCode errorCode;
}
