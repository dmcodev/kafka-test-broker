package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.meta.Field;
import dev.dmco.test.kafka.messages.meta.SinceVersion;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class ResponseHeader {

    @Field(FieldType.INT32)
    Integer correlationId;

    @SinceVersion(1)
    @Field(FieldType.TAGS_BUFFER)
    byte[] tagsBuffer;
}
