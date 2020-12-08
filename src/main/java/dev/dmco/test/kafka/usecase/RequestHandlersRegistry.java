package dev.dmco.test.kafka.usecase;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.messages.response.ResponseMessage;

import java.util.HashMap;
import java.util.Map;

public class RequestHandlersRegistry {

    private final Map<Class<?>, RequestHandler<?, ?>> handlers = new HashMap<>();

    public RequestHandlersRegistry() {
        RequestHandler.loadAll().forEach(this::register);
    }

    @SuppressWarnings("unchecked")
    public RequestHandler<RequestMessage, ResponseMessage> selectHandler(RequestMessage request) {
        Class<? extends RequestMessage> requestType = request.getClass();
        while (RequestMessage.class.isAssignableFrom(requestType)) {
            if (handlers.containsKey(requestType)) {
                return (RequestHandler<RequestMessage, ResponseMessage>) handlers.get(requestType);
            }
            requestType = (Class<? extends RequestMessage>) requestType.getSuperclass();
        }
        throw new BrokerException(
            "No matching handler found for request message of type " + request.getClass(),
            ErrorCode.INVALID_REQUEST
        );
    }

    private void register(RequestHandler<?, ?> handler) {
        handlers.put(RequestHandler.getHandledRequestType(handler), handler);
    }
}
