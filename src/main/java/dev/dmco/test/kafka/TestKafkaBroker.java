package dev.dmco.test.kafka;

import dev.dmco.test.kafka.config.BrokerConfig;
import dev.dmco.test.kafka.io.IOEventLoop;
import dev.dmco.test.kafka.state.BrokerState;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TestKafkaBroker implements AutoCloseable {

    private final BrokerState state;
    private final IOEventLoop eventLoop;

    public TestKafkaBroker() {
        this(BrokerConfig.createDefault());
    }

    public TestKafkaBroker(BrokerConfig config) {
        state = new BrokerState(config);
        eventLoop = new IOEventLoop(state);
    }

    @Override
    public void close() {
        eventLoop.close();
    }
}
