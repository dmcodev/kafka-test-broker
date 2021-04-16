package dev.dmcode.test.kafka.usecase.coordinator;

import dev.dmcode.test.kafka.messages.Tag;
import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.Collection;

@Request(key = 10, maxVersion = 2)
@lombok.Value
@Accessors(fluent = true)
public class FindCoordinatorRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 3)
    RequestHeader header;

    // @TypeOverride(value = ValueType.COMPACT_STRING, sinceApiVersion = 3)
    String key;

    @SinceVersion(1)
    byte keyType;

    @SinceVersion(3)
    Collection<Tag> tags;
}
