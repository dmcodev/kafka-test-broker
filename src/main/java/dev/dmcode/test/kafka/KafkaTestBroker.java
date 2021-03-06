package dev.dmcode.test.kafka;

import dev.dmcode.test.kafka.config.BrokerConfig;
import dev.dmcode.test.kafka.io.Connection;
import dev.dmcode.test.kafka.io.EventLoopAction;
import dev.dmcode.test.kafka.io.RequestDecoder;
import dev.dmcode.test.kafka.io.ResponseEncoder;
import dev.dmcode.test.kafka.logging.Logger;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import dev.dmcode.test.kafka.messages.response.ResponseMessage;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.query.BrokerQuery;
import dev.dmcode.test.kafka.state.query.QueryExecutor;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaTestBroker implements AutoCloseable {

    private static final Logger LOG = Logger.create(KafkaTestBroker.class);
    private static final AtomicInteger THREAD_ID_SEQUENCE = new AtomicInteger();
    private static final int SELECT_TIMEOUT = 10;

    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ResponseEncoder encoder = new ResponseEncoder();
    private final BlockingQueue<EventLoopAction<?>> actions = new ArrayBlockingQueue<>(256, true);

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final SelectionKey serverSelectionKey;
    private final ExecutorService executorService;

    private final BrokerState state;
    private final RequestDecoder decoder;

    public KafkaTestBroker() {
        this(BrokerConfig.createDefault());
    }

    @SneakyThrows
    public KafkaTestBroker(BrokerConfig config) {
        state = new BrokerState(config);
        decoder = new RequestDecoder(state.requestHandlers());
        try {
            selector = Selector.open();
            serverChannel = createServerChannel();
            serverSelectionKey = serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            executorService = startExecutor();
            bindServerChannel();
        } catch (Exception error) {
            close();
            throw error;
        }
    }

    public BrokerQuery query() {
        return new BrokerQuery(() -> state, new QueryExecutor() {
            @Override
            public <T> T execute(Supplier<T> query) {
                return KafkaTestBroker.this.execute(query::get);
            }
        });
    }

    public void reset() {
        execute(this::resetInternal);
    }

    @Override
    @SneakyThrows
    public void close() {
        if (closed.compareAndSet(false, true)) {
            awaitStop();
            closeSockets();
            closeSelector();
            closeActions();
            closeExecutorService();
        }
    }

    private void eventLoop() {
        try {
            while (!closed.get()) {
                processSelector();
                processActions();
            }
        } catch (Exception error) {
            LOG.debug("Event loop error, closing broker", error);
        } finally {
            stopped.countDown();
            close();
        }
    }

    private void processActions() {
        getScheduledActions().forEach(EventLoopAction::run);
    }

    private List<EventLoopAction<?>> getScheduledActions() {
        List<EventLoopAction<?>> scheduledActions = actions.stream()
            .filter(EventLoopAction::scheduledForNow)
            .collect(Collectors.toList());
        scheduledActions.forEach(actions::remove);
        return scheduledActions;
    }

    private void processSelector() throws IOException {
        int numberOfSelectedKeys = selector.select(SELECT_TIMEOUT);
        if (numberOfSelectedKeys > 0 && !closed.get()) {
            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey selectionKey = selectedKeys.next();
                if (selectionKey.isValid()) {
                    processSelectionKey(selectionKey);
                }
                selectedKeys.remove();
            }
        }
    }

    private void processSelectionKey(SelectionKey selectionKey) {
        try {
            if (selectionKey.isAcceptable()) {
                onClientConnectionInitializing();
            }
            if (selectionKey.isReadable()) {
                onClientConnectionReadable(selectionKey);
            }
            if (selectionKey.isWritable()) {
                onClientConnectionWritable(selectionKey);
            }
        } catch (ClosedChannelException closedChannelException) {
            closeSelectionKey(selectionKey);
        } catch (Exception error) {
            LOG.debug("Uncaught error, closing connection", error);
            closeSelectionKey(selectionKey);
        }
    }

    private void onClientConnectionInitializing() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        Connection connection = new Connection(clientChannel);
        clientChannel.register(selector, SelectionKey.OP_READ, connection);
    }

    private void onClientConnectionReadable(SelectionKey selectionKey) throws IOException {
        Connection connection = (Connection) selectionKey.attachment();
        connection.readRequests()
            .stream()
            .map(decoder::decode)
            .forEach(request -> handleRequest(request, selectionKey));
    }

    private void onClientConnectionWritable(SelectionKey selectionKey) {
        selectionKey.interestOps(selectionKey.interestOps() & (~SelectionKey.OP_WRITE));
        writeResponses(selectionKey);
    }

    private void handleRequest(RequestMessage request, SelectionKey selectionKey) {
        Connection connection = (Connection) selectionKey.attachment();
        connection.addRequestCorrelationId(request.header().correlationId());
        ResponseScheduler<ResponseMessage> responseScheduler = new EventLoopResponseScheduler(request, selectionKey);
        state.requestHandlers()
            .select(request)
            .handle(request, state, responseScheduler);
    }

    private void enqueueResponse(RequestMessage request, ResponseMessage response, SelectionKey selectionKey) {
        Connection connection = (Connection) selectionKey.attachment();
        ByteBuffer responseBuffer = encoder.encode(response, request.header());
        connection.enqueueResponse(request.header().correlationId(), responseBuffer);
        writeResponses(selectionKey);
    }

    private void writeResponses(SelectionKey selectionKey) {
        Connection connection = (Connection) selectionKey.attachment();
        if (!connection.writeResponses()) {
            selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private ServerSocketChannel createServerChannel() throws IOException {
        ServerSocketChannel channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        return channel;
    }

    private void bindServerChannel() throws IOException {
        BrokerConfig config = state.config();
        InetSocketAddress bindAddress = new InetSocketAddress(config.host(), config.port());
        serverChannel.bind(bindAddress);
    }

    private ExecutorService startExecutor() {
        ExecutorService executor = Executors.newSingleThreadExecutor(KafkaTestBroker::createEventLoopThread);
        executor.execute(this::eventLoop);
        return executor;
    }

    private void execute(Runnable action) {
        execute(Executors.callable(action));
    }

    @SneakyThrows
    private <T> T execute(Callable<T> action) {
        EventLoopAction<T> eventLoopAction = new EventLoopAction<>(action);
        if (closed.get()) {
            return eventLoopAction.close().getResult();
        }
        actions.put(eventLoopAction);
        if (closed.get()) {
            actions.remove(eventLoopAction);
            eventLoopAction.close();
        } else {
            selector.wakeup();
        }
        return eventLoopAction.getResult();
    }

    private void resetInternal() {
        state.reset();
        closeClientConnections();
    }

    @SneakyThrows
    private void awaitStop() {
        if (executorService != null) {
            stopped.await();
        }
    }

    private void closeSockets() {
        Optional.ofNullable(serverSelectionKey).ifPresent(SelectionKey::cancel);
        closeClientConnections();
        Optional.ofNullable(serverChannel)
            .filter(AbstractInterruptibleChannel::isOpen)
            .ifPresent(this::closeChannel);
    }

    private void closeClientConnections() {
        try {
            selector.keys().stream()
                .filter(key -> !key.equals(serverSelectionKey))
                .forEach(this::closeSelectionKey);
        } catch (ClosedSelectorException closedSelectorException) {
            LOG.debug("Could not close selection keys, selector already closed");
        }
    }

    private void closeSelectionKey(SelectionKey selectionKey) {
        selectionKey.cancel();
        closeChannel(selectionKey.channel());
    }

    private void closeChannel(SelectableChannel closingChannel) {
        try {
            closingChannel.close();
        } catch (Exception error) {
            LOG.debug("Error while closing socket channel", error);
        }
    }

    private void closeSelector() {
        if (selector != null) {
            try {
                selector.close();
            } catch (Exception error) {
                LOG.debug("Error while closing selector", error);
            }
        }
    }

    private void closeActions() {
        getScheduledActions().forEach(EventLoopAction::close);
    }

    @SneakyThrows
    private void closeExecutorService() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public static Thread createEventLoopThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("kafka-test-broker-" + THREAD_ID_SEQUENCE.incrementAndGet());
        return thread;
    }

    @RequiredArgsConstructor
    private class EventLoopResponseScheduler implements ResponseScheduler<ResponseMessage> {

        final RequestMessage request;
        final SelectionKey selectionKey;

        @Override
        public void scheduleResponse(ResponseMessage response) {
            enqueueResponse(request, response, selectionKey);
        }

        @Override
        public void scheduleResponse(long delay, ResponseMessage response) {
            schedule(delay, () -> enqueueResponse(request, response, selectionKey));
        }

        @Override
        @SneakyThrows
        public void schedule(long delay, Runnable runnable) {
            Callable<?> callable = Executors.callable(runnable);
            long scheduleTimestamp = System.currentTimeMillis() + delay;
            EventLoopAction<?> action = new EventLoopAction<>(callable, scheduleTimestamp);
            actions.put(action);
        }
    }
}
