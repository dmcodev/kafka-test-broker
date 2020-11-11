package dev.dmco.test.kafka;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class TestKafkaBrokerConfig {

    @Builder.Default
    String host = "0.0.0.0";

    @Builder.Default
    int port = 9092;

    public static TestKafkaBrokerConfig getDefault() {
        return TestKafkaBrokerConfig.builder().build();
    }
}
