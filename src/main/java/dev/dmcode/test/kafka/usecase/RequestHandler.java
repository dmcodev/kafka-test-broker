package dev.dmcode.test.kafka.usecase;

import dev.dmcode.test.kafka.io.codec.registry.Type;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import dev.dmcode.test.kafka.messages.response.ResponseMessage;
import dev.dmcode.test.kafka.state.BrokerState;

import java.util.Arrays;
import java.util.Collection;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public interface RequestHandler<IN extends RequestMessage, OUT extends ResponseMessage> {

    void handle(IN request, BrokerState state, ResponseScheduler<OUT> scheduler);

    static Collection<RequestHandler<?, ?>> loadAll() {
        return StreamSupport.stream(ServiceLoader.load(RequestHandler.class).spliterator(), false)
            .map(it -> (RequestHandler<?, ?>) it)
            .collect(Collectors.toList());
    }

    static Class<?> getHandledRequestType(RequestHandler<?, ?> handler) {
        return Arrays.stream(handler.getClass().getGenericInterfaces())
            .map(Type::of)
            .filter(it -> it.raw().equals(RequestHandler.class))
            .map(it -> it.typeParameters().get(0).raw())
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Could not determine supported request type for handler: " + handler.getClass()));
    }
}
