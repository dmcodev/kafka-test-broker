package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.metadata.ApiVersionMapping;
import dev.dmco.test.kafka.messages.metadata.SinceApiVersion;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
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

    @ApiVersionMapping(value = 0, sinceApiVersion = 0)
    ResponseHeader header;

    short errorCode;

    List<ApiKey> apiKeys;

    @SinceApiVersion(2)
    int throttleTimeMs;

    @SinceApiVersion(3)
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
