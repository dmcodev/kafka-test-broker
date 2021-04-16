package dev.dmcode.test.kafka.state.query;

import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.Topic;
import dev.dmcode.test.kafka.state.query.deserializer.RecordDeserializer;
import dev.dmcode.test.kafka.state.query.view.RecordView;
import lombok.RequiredArgsConstructor;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class TopicQuery {

    private final String name;
    private final Supplier<BrokerState> state;
    private final QueryExecutor executor;

    public boolean exists() {
        Supplier<Boolean> query = () -> getTopic(name).isPresent();
        return executor.execute(query);
    }

    public int getNumberOfPartitions() {
        Supplier<Integer> query = () -> getTopicOrThrow(name).getNumberOfPartitions();
        return executor.execute(query);
    }

    public RecordSetQuery<byte[], byte[], byte[]> selectRecords() {
        RecordDeserializer<byte[], byte[], byte[]> deserializer = RecordDeserializer.bytes();
        Supplier<Stream<RecordView<byte[], byte[], byte[]>>> records = () -> getTopicOrThrow(name)
            .getPartitions()
            .stream()
            .flatMap(partition -> partition.getRecords().stream()
                .map(record -> RecordView.create(partition.getId(), record, deserializer))
            );
        return new RecordSetQuery<>(records, deserializer, executor);
    }

    private Optional<Topic> getTopic(String name) {
        return state.get().getTopic(name);
    }

    private Topic getTopicOrThrow(String name) {
        return getTopic(name)
            .orElseThrow(() -> new IllegalArgumentException("Topic does not exist: " + name));
    }
}
