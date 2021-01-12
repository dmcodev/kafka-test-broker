package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.config.BrokerConfig;
import dev.dmco.test.kafka.logging.Logger;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import dev.dmco.test.kafka.state.BrokerState;
import lombok.SneakyThrows;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventLoop implements AutoCloseable {

    private static final Logger LOG = Logger.create(EventLoop.class);
    private static final int SELECT_TIMEOUT = 1000;

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final ExecutorService executorService;

    private final BrokerState state;
    private final RequestDecoder decoder;

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ResponseEncoder encoder = new ResponseEncoder();

    @SneakyThrows
    public EventLoop(BrokerState state) {
        this.state = state;
        decoder = new RequestDecoder(state.requestHandlers());
        try {
            selector = Selector.open();
            serverChannel = openServerChannel();
            executorService = startExecutor();
        } catch (Exception error) {
            close();
            throw error;
        }
    }

    private void eventLoop() {
        try {
            while (!stopped.get()) {
                int numberOfSelectedKeys = selector.select(SELECT_TIMEOUT);
                if (numberOfSelectedKeys > 0 && !stopped.get()) {
                    Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                    while (selectedKeys.hasNext()) {
                        SelectionKey selectionKey = selectedKeys.next();
                        if (selectionKey.isValid()) {
                            handleSelectionKey(selectionKey);
                        }
                        selectedKeys.remove();
                    }
                }
            }
        } catch (Exception error) {
            LOG.error("Uncaught error, closing broker", error);
            close();
        }
    }

    private void handleSelectionKey(SelectionKey selectionKey) {
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
            LOG.error("Uncaught error, closing connection", error);
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
            .forEach(request -> handleRequest(request, connection, selectionKey));
    }

    private void onClientConnectionWritable(SelectionKey selectionKey) {
        selectionKey.interestOps(selectionKey.interestOps() & (~SelectionKey.OP_WRITE));
        writeResponses(selectionKey);
    }

    private void handleRequest(RequestMessage request, Connection connection, SelectionKey selectionKey) {
        ResponseMessage response = state.requestHandlers()
            .select(request)
            .handle(request, state);
        ByteBuffer responseBuffer = encoder.encode(response, request.header());
        connection.enqueueResponse(responseBuffer);
        writeResponses(selectionKey);
    }

    private void writeResponses(SelectionKey selectionKey) {
        Connection connection = (Connection) selectionKey.attachment();
        if (!connection.writeResponses()) {
            selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private ServerSocketChannel openServerChannel() throws IOException {
        BrokerConfig config = state.config();
        InetSocketAddress bindAddress = new InetSocketAddress(config.host(), config.port());
        ServerSocketChannel channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_ACCEPT);
        channel.bind(bindAddress);
        return channel;
    }

    private ExecutorService startExecutor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(this::eventLoop);
        return executor;
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            closeSelector();
            stopExecutorService();
        }
    }

    private void closeSelector() {
        if (selector != null) {
            try {
                selector.wakeup();
                selector.keys()
                    .forEach(this::closeSelectionKey);
                selector.close();
            } catch (Exception error) {
                LOG.warn("Error while closing selector", error);
            }
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
            LOG.warn("Error while closing socket channel", error);
        }
    }

    @SneakyThrows
    private void stopExecutorService() {
        if (executorService != null) {
            executorService.shutdown();
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                LOG.warn("Timeout while waiting for closing of broker event loop executor");
            }
        }
    }
}
