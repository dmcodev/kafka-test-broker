package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.meta.FieldSequence;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.Struct;
import dev.dmco.test.kafka.messages.meta.VersionOverride;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
@Request(apiKey = 3, maxVersion = 1)
public class MetadataRequest implements RequestMessage {

    @Struct
    @VersionOverride(value = 1, sinceVersion = 0)
    @VersionOverride(value = 2, sinceVersion = 9)
    RequestHeader header;

    @FieldSequence(FieldType.STRING)
    List<String> topicNames;

//    @SinceVersion(4)
//    @Field(FieldType.BOOLEAN)
//    Boolean allowAutoTopicCreation;
//
//    @SinceVersion(8)
//    @Field(FieldType.BOOLEAN)
//    Boolean includeClusterAuthorizedOperations;
//
//    @SinceVersion(8)
//    @Field(FieldType.BOOLEAN)
//    Boolean includeTopicAuthorizedOperations;
}
