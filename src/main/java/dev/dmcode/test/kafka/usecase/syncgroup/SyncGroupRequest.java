package dev.dmcode.test.kafka.usecase.syncgroup;

import dev.dmcode.test.kafka.messages.consumer.Assignment;
import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Request(key = 14)
@lombok.Value
@Accessors(fluent = true)
public class SyncGroupRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 4)
    RequestHeader header;

    String groupId;

    int generationId;

    String memberId;

    List<MemberAssignment> memberAssignments;

    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class MemberAssignment {

        String memberId;

        Assignment assignment;
    }
}
