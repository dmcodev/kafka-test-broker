package dev.dmco.test.kafka;

import dev.dmco.test.kafka.io.IOEventLoop;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandlersRegistry;
import lombok.RequiredArgsConstructor;

import java.net.InetSocketAddress;

@RequiredArgsConstructor
public class TestKafkaBroker implements AutoCloseable {

    private final RequestHandlersRegistry handlersRegistry = new RequestHandlersRegistry();
    private final BrokerState state;
    private final IOEventLoop eventLoop;

    public TestKafkaBroker() {
        this(TestKafkaBrokerConfig.getDefault());
    }

    public TestKafkaBroker(TestKafkaBrokerConfig config) {
        InetSocketAddress bindAddress = new InetSocketAddress(config.host(), config.port());
        state = new BrokerState(config);
        eventLoop = new IOEventLoop(bindAddress, this::handleRequest);
    }

    private ResponseMessage handleRequest(RequestMessage request) {
        return handlersRegistry.selectHandler(request)
            .handle(request, state);
    }


    @Override
    public void close() {
        eventLoop.close();
    }
}
