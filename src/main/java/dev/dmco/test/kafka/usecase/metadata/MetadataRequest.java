package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.ValueSequence;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Request(apiKey = 3)
@ApiVersion(max = 1)
@Value
@Accessors(fluent = true)
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
