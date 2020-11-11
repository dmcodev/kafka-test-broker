package dev.dmco.test.kafka;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TestKafkaBroker implements AutoCloseable {

    private final IOEventLoop eventLoop;

    public TestKafkaBroker() {
        this(TestKafkaBrokerConfig.getDefault());
    }

    public TestKafkaBroker(TestKafkaBrokerConfig config) {
        eventLoop = new IOEventLoop(config);
    }

    @Override
    public void close() {
        eventLoop.close();
    }
}
