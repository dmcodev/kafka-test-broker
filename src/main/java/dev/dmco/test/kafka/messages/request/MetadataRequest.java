package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.meta.FieldSequence;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.Struct;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
@Request(apiKey = 3, maxVersion = 1)
public class MetadataRequest implements RequestMessage {

    @Struct
    RequestHeader header;

    @FieldSequence(FieldType.NULLABLE_STRING)
    List<String> topicNames;
}
