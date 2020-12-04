package dev.dmco.test.kafka.usecase.joingroup;

import dev.dmco.test.kafka.messages.Subscription;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class JoinGroupResponse implements ResponseMessage {

    @VersionMapping(value = 0, sinceVersion = 0)
    @VersionMapping(value = 1, sinceVersion = 6)
    ResponseHeader header;

    short errorCode;

    int generationId;

    String protocolName;

    String leader;

    String memberId;

    @Singular
    List<Member> members;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Member {

        String memberId;

        Subscription subscription;
    }
}
