package dev.dmco.test.kafka;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class TestKafkaBrokerConfig {

    @Builder.Default
    String host = "localhost";

    @Builder.Default
    int port = 9092;

    @Value
    @Builder
    @Accessors(fluent = true)
    public static class TopicConfig {

    }

    public static TestKafkaBrokerConfig getDefault() {
        return TestKafkaBrokerConfig.builder().build();
    }
}
