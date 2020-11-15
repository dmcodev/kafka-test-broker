package dev.dmco.test.kafka.messages;

import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import dev.dmco.test.kafka.state.BrokerState;

import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public interface RequestHandler<IN extends RequestMessage, OUT extends ResponseMessage> {

    List<Class<? extends IN>> handledRequestTypes();

    OUT handle(IN request, BrokerState state);

    static Collection<RequestHandler<?, ?>> loadAll() {
        return StreamSupport.stream(ServiceLoader.load(RequestHandler.class).spliterator(), false)
            .map(it -> (RequestHandler<?, ?>) it)
            .collect(Collectors.toList());
    }
}
