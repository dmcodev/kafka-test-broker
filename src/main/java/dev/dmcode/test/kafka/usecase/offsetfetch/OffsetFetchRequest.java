package dev.dmcode.test.kafka.usecase.offsetfetch;

import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Request(key = 9)
@lombok.Value
@Accessors(fluent = true)
public class OffsetFetchRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 6)
    RequestHeader header;

    String groupId;

    List<Topic> topics;

    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Topic {

        String name;

        List<Integer> partitionIds;
    }
}
