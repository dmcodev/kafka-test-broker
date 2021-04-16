package dev.dmcode.test.kafka.state.query;

import java.util.function.Supplier;

@FunctionalInterface
public interface QueryExecutor {

    <T> T execute(Supplier<T> query);
}
