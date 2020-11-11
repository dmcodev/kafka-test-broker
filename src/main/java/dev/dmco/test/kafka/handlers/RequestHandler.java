package dev.dmco.test.kafka.handlers;

import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.state.BrokerState;

import java.util.List;

public interface RequestHandler<IN extends RequestMessage, OUT extends ResponseMessage> {

    List<Class<? extends IN>> handledRequestTypes();

    OUT handle(IN request, BrokerState state);
}
