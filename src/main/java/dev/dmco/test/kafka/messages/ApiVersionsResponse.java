package dev.dmco.test.kafka.messages;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Builder
@Accessors(fluent = true)
public class ApiVersionsResponse implements ResponseMessage {

    int errorCode;
    @Singular List<ApiKey> apiKeys;

    @Value
    @AllArgsConstructor(staticName = "from")
    @Accessors(fluent = true)
    public static class ApiKey {
        int apiKey;
        int minVersion;
        int maxVersion;
    }
}
