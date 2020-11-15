package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.ValueSequence;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
@Request(apiKey = 3, maxVersion = 1)
public class MetadataRequest implements RequestMessage {

    @ApiVersionOverride(value = 1, sinceVersion = 0)
    @ApiVersionOverride(value = 2, sinceVersion = 9)
    RequestHeader header;

    @ValueSequence(ValueType.STRING)
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
