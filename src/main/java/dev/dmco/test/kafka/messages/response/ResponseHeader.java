package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;

import java.util.Collection;

@lombok.Value
@Builder
@With
@AllArgsConstructor
public class ResponseHeader {

    int correlationId;

    @ApiVersion(min = 1)
    Collection<Tag> tags;
}
