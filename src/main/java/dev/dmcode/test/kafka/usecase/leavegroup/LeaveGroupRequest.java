package dev.dmcode.test.kafka.usecase.leavegroup;

import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

@Request(key = 13)
@lombok.Value
@Accessors(fluent = true)
public class LeaveGroupRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 4)
    RequestHeader header;

    String groupId;

    String memberId;
}
