package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.List;

@HeaderVersion(value = 0, sinceApiVersion = 0)
@lombok.Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class ApiVersionsResponse implements ResponseMessage {

    @Value(ValueType.RESPOSNE_HEADER)
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
