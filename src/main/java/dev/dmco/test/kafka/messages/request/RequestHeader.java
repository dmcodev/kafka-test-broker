package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.meta.Field;
import dev.dmco.test.kafka.messages.meta.SinceVersion;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class RequestHeader {

    @Field(FieldType.INT16)
    Short apiKey;

    @Field(FieldType.INT16)
    Short apiVersion;

    @Field(FieldType.INT32)
    Integer correlationId;

    @SinceVersion(1)
    @Field(FieldType.NULLABLE_STRING)
    String clientId;

    @SinceVersion(2)
    @Field(FieldType.TAGS_BUFFER)
    byte[] tagsBuffer;
}
