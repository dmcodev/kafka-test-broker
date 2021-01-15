package dev.dmco.test.kafka.io;

import lombok.Value;
import lombok.experimental.Accessors;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Value
@Accessors(fluent = true)
public class EventLoopAction<T> {

    CompletableFuture<T> future = new CompletableFuture<>();
    Callable<T> action;

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
        future.completeExceptionally(new IllegalStateException("Could not execute action, event loop has been closed"));
        return this;
    }
}
