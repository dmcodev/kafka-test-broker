package dev.dmcode.test.kafka.messages.response;

import dev.dmcode.test.kafka.messages.Tag;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
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

    @SinceVersion(1)
    Collection<Tag> tags;
}
