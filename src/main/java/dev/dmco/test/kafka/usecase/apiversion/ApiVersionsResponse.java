package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class ApiVersionsResponse implements ResponseMessage {

    @ApiVersionOverride(value = 0, sinceVersion = 0)
    ResponseHeader header;

    @Value(ValueType.INT16)
    Short errorCode;

    @StructSequence(ApiKey.class)
    List<ApiKey> apiKeys;

    @ApiVersion(min = 2)
    @Value(ValueType.INT32)
    Integer throttleTimeMs;

    @ApiVersion(min = 3)
    @Value(ValueType.TAGS_BUFFER)
    byte[] tagsBuffer;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class ApiKey {

        @Value(ValueType.INT16)
        Short apiKey;

        @Value(ValueType.INT16)
        Short minVersion;

        @Value(ValueType.INT16)
        Short maxVersion;
    }
}
