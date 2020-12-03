package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.metadata.SinceApiVersion;
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

    @SinceApiVersion(1)
    Collection<Tag> tags;
}
