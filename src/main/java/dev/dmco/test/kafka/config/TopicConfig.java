package dev.dmco.test.kafka.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@AllArgsConstructor(staticName = "create")
@Accessors(fluent = true)
public class TopicConfig {

    String name;

    @Builder.Default
    int partitionsNumber = 1;

    public static TopicConfig createDefault(String name) {
        return builder().name(name).build();
    }
}
