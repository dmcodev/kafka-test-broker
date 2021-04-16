package dev.dmco.test.kafka.state.query;

import dev.dmco.test.kafka.state.Topic;
import lombok.RequiredArgsConstructor;

import java.util.Optional;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class TopicQuery {

    private final String name;
    private final Supplier<Optional<Topic>> input;
    private final QueryExecutor executor;

    public boolean exists() {
        Supplier<Boolean> query = () -> input.get().isPresent();
        return executor.execute(query);
    }

    public int numberOfPartitions() {
        Supplier<Integer> query = () -> getTopic().getNumberOfPartitions();
        return executor.execute(query);
    }

    private Topic getTopic() {
        return input.get()
            .orElseThrow(() -> new IllegalArgumentException("Topic does not exist: " + name));
    }
}
