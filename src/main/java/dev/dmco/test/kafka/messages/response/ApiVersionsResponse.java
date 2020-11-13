package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.messages.meta.Field;
import dev.dmco.test.kafka.messages.meta.SinceVersion;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class ApiVersionsResponse implements ResponseMessage {

    @Field(FieldType.INT16)
    Short errorCode;

    @StructSequence(ApiKey.class)
    List<ApiKey> apiKeys;

    @SinceVersion(2)
    @Field(FieldType.INT32)
    Integer throttleTimeMs;

    @SinceVersion(3)
    @Field(FieldType.TAGS_BUFFER)
    byte[] tagsBuffer;

    @Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class ApiKey {

        @Field(FieldType.INT16)
        Short apiKey;

        @Field(FieldType.INT16)
        Short minVersion;

        @Field(FieldType.INT16)
        Short maxVersion;
    }
}
