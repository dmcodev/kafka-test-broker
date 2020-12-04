package dev.dmco.test.kafka.usecase.joingroup;

import dev.dmco.test.kafka.messages.common.Subscription;
import dev.dmco.test.kafka.messages.metadata.ApiVersion;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Request(apiKey = 11)
@ApiVersion(max = 0)
@lombok.Value
@Accessors(fluent = true)
public class JoinGroupRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceApiVersion = 0)
    @VersionMapping(value = 2, sinceApiVersion = 6)
    RequestHeader header;

    String groupId;

    int sessionTimeoutMs;

    String memberId;

    String protocolType;

    List<Protocol> protocols;

    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Protocol {

        String name;

        Subscription subscription;
    }
}
