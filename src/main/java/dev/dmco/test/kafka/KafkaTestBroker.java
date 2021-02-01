package dev.dmco.test.kafka;

import dev.dmco.test.kafka.config.BrokerConfig;
import dev.dmco.test.kafka.io.EventLoop;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class KafkaTestBroker implements AutoCloseable {

    private final EventLoop eventLoop;

    public KafkaTestBroker() {
        this(BrokerConfig.createDefault());
    }

    public KafkaTestBroker(BrokerConfig config) {
        eventLoop = new EventLoop(config);
    }

    public void reset() {
        eventLoop.reset();
    }

    @Override
    public void close() {
        eventLoop.close();
    }
}
