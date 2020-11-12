package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.messages.data.TaggedFields;
import dev.dmco.test.kafka.messages.meta.Sequence;
import dev.dmco.test.kafka.messages.meta.SinceApiVersion;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

@Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class ApiVersionsResponse implements ResponseMessage {

    short errorCode;

    @Sequence(ApiKey.class)
    @Builder.Default
    List<ApiKey> apiKeys = Collections.emptyList();

    @SinceApiVersion(2)
    int throttleTimeMs;

    @SinceApiVersion(3)
    @Builder.Default
    TaggedFields taggedFields = new TaggedFields();

    @Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class ApiKey {
        short apiKey;
        short minVersion;
        short maxVersion;
    }
}
