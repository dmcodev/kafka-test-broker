package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RequestHandlers {

    private final Map<Class<?>, RequestHandler<RequestMessage, ResponseMessage>> handlers = new HashMap<>();

    public RequestHandlers() {
        RequestHandler.loadAll().forEach(this::register);
    }

    public RequestHandler<RequestMessage, ResponseMessage> selectHandler(RequestMessage request) {
        return Optional.ofNullable(handlers.get(request.getClass()))
            .orElseThrow(() -> onMissingHandler(request));
    }

    private RuntimeException onMissingHandler(RequestMessage request) {
        throw new IllegalStateException("No matching handler found for request message of type " + request.getClass());
    }

    @SuppressWarnings("unchecked")
    private void register(RequestHandler<?, ?> handler) {
        handlers.put(RequestHandler.getHandledRequestType(handler), (RequestHandler<RequestMessage, ResponseMessage>) handler);
    }
}
