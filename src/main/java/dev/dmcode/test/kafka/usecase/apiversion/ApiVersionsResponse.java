package dev.dmcode.test.kafka.usecase.apiversion;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.messages.Tag;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.response.ResponseHeader;
import dev.dmcode.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.List;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class ApiVersionsResponse implements ResponseMessage {

    @VersionMapping(value = 0, sinceVersion = 0)
    ResponseHeader header;

    ErrorCode errorCode;

    List<ApiKey> apiKeys;

    @SinceVersion(2)
    int throttleTimeMs;

    @SinceVersion(3)
    Collection<Tag> tags;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class ApiKey {

        short apiKey;

        short minVersion;

        short maxVersion;
    }
}
