package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.TestKafkaBrokerConfig;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
public class BrokerState {

    private final int nodeId = 1;

    private final TestKafkaBrokerConfig config;
}
