package dev.dmco.test.kafka;

import dev.dmco.test.kafka.config.BrokerConfig;
import dev.dmco.test.kafka.io.EventLoop;
import dev.dmco.test.kafka.state.BrokerState;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TestKafkaBroker implements AutoCloseable {

    private final BrokerState state;
    private final EventLoop eventLoop;

    public TestKafkaBroker() {
        this(BrokerConfig.createDefault());
    }

    public TestKafkaBroker(BrokerConfig config) {
        state = new BrokerState(config);
        eventLoop = new EventLoop(state);
    }

    public void reset() {
        eventLoop.execute(state::reset);
        eventLoop.reset();
    }

    @Override
    public void close() {
        eventLoop.close();
    }
}
