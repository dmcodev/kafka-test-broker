package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.handlers.RequestHandler;
import dev.dmco.test.kafka.messages.KafkaRequest;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.stream.Collectors;

@Getter
@Accessors(fluent = true)
public class BrokerState {

    private final RequestHandlersRegistry handlersRegistry = new RequestHandlersRegistry();

    public Collection<Class<?>> getSupportedRequestTypes() {
        return handlersRegistry.getHandlers().stream()
            .map(RequestHandler::handledRequestTypes)
            .flatMap(Collection::stream)
            .filter(type -> type.isAnnotationPresent(KafkaRequest.class))
            .collect(Collectors.toList());
    }
}
