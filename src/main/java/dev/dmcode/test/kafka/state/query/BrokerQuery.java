package dev.dmcode.test.kafka.state.query;

import dev.dmcode.test.kafka.state.BrokerState;
import lombok.RequiredArgsConstructor;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class BrokerQuery {

    private final Supplier<BrokerState> state;
    private final QueryExecutor executor;

    public TopicQuery selectTopic(String name) {
        return new TopicQuery(name, state, executor);
    }
}
