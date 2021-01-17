package dev.dmco.test.kafka.usecase;

import dev.dmco.test.kafka.messages.response.ResponseMessage;

public interface ResponseScheduler<OUT extends ResponseMessage> {

    void scheduleResponse(OUT response);

    void scheduleResponse(long delay, OUT response);

    void schedule(long delay, Runnable runnable);
}
