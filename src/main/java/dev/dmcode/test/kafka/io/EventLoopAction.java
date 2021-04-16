package dev.dmcode.test.kafka.io;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Value
@RequiredArgsConstructor
@Accessors(fluent = true)
public class EventLoopAction<T> {

    CompletableFuture<T> future = new CompletableFuture<>();
    Callable<T> action;
    long scheduleTimestamp;

    public EventLoopAction(Callable<T> action) {
        this(action, System.currentTimeMillis());
    }

    public boolean scheduledForNow() {
        return System.currentTimeMillis() >= scheduleTimestamp;
    }

    public void run() {
        try {
            future.complete(action.call());
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
    }

    public T getResult() throws ExecutionException, InterruptedException {
        return future.get();
    }

    public EventLoopAction<T> close() {
        future.completeExceptionally(new IllegalStateException("Broker is closed"));
        return this;
    }
}
