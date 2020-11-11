package dev.dmco.test.kafka;

import dev.dmco.test.kafka.io.IOEventLoop;
import dev.dmco.test.kafka.state.BrokerState;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TestKafkaBroker implements AutoCloseable {

    private final IOEventLoop eventLoop;

    public TestKafkaBroker() {
        this(TestKafkaBrokerConfig.getDefault());
    }

    public TestKafkaBroker(TestKafkaBrokerConfig config) {
        BrokerState state = new BrokerState();
        eventLoop = new IOEventLoop(config, state);
    }

    @Override
    public void close() {
        eventLoop.close();
    }
}
