package dev.dmcode.test.kafka.usecase.joingroup;

import dev.dmcode.test.kafka.messages.consumer.Subscription;
import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Request(key = 11)
@lombok.Value
@Accessors(fluent = true)
public class JoinGroupRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 6)
    RequestHeader header;

    String groupId;

    int sessionTimeoutMs;

    String memberId;

    String protocolFamily;

    List<Protocol> protocols;

    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Protocol {

        String name;

        Subscription subscription;
    }
}
