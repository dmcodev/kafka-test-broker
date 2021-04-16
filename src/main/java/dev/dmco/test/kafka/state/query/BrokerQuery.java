package dev.dmco.test.kafka.state.query;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.Topic;
import lombok.RequiredArgsConstructor;

import java.util.Optional;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class BrokerQuery {

    private final Supplier<BrokerState> input;
    private final QueryExecutor executor;

    public TopicQuery topic(String name) {
        Supplier<Optional<Topic>> query = () -> input.get().getTopic(name);
        return new TopicQuery(name, query, executor);
    }
}
