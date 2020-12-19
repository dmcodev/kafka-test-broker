package dev.dmco.test.kafka.config;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Builder
@Accessors(fluent = true)
public class BrokerConfig {

    @Builder.Default
    String host = "localhost";

    @Builder.Default
    int port = 9092;

    @Singular
    List<TopicConfig> topics;

    public static BrokerConfig createDefault() {
        return BrokerConfig.builder().build();
    }
}
