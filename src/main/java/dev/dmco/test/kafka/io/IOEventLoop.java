package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.config.BrokerConfig;
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

public class IOEventLoop implements AutoCloseable {

    private static final int SELECT_TIMEOUT = 1000;

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final ExecutorService executorService;

    private final BrokerState state;

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final IOEncoder encoder = new IOEncoder();
    private final IODecoder decoder = new IODecoder();

    @SneakyThrows
    public IOEventLoop(BrokerState state) {
        this.state = state;
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
        while (!stopped.get()) {
            int numberOfSelectedKeys;
            try {
                numberOfSelectedKeys = selector.select(SELECT_TIMEOUT);
            } catch (Exception error) {
                System.err.println("Selector error");
                error.printStackTrace();
                close();
                break;
            }
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
            System.err.println("Connection error");
            error.printStackTrace();
            closeSelectionKey(selectionKey);
        }
    }

    private void onClientConnectionInitializing() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        IOSession ioSession = new IOSession(clientChannel);
        clientChannel.register(selector, SelectionKey.OP_READ, ioSession);
    }

    private void onClientConnectionReadable(SelectionKey selectionKey) throws IOException {
        IOSession ioSession = (IOSession) selectionKey.attachment();
        ioSession.readRequests()
            .stream()
            .map(decoder::decode)
            .forEach(request -> handleRequest(request, ioSession, selectionKey));
    }

    private void onClientConnectionWritable(SelectionKey selectionKey) {
        selectionKey.interestOps(selectionKey.interestOps() & (~SelectionKey.OP_WRITE));
        writeResponses(selectionKey);
    }

    private void handleRequest(RequestMessage request, IOSession ioSession, SelectionKey selectionKey) {
        ResponseMessage response = state.selectRequestHandler(request)
            .handle(request, state);
        ByteBuffer responseBuffer = encoder.encode(response, request.header());
        ioSession.enqueueResponse(responseBuffer);
        writeResponses(selectionKey);
    }

    private void writeResponses(SelectionKey selectionKey) {
        IOSession ioSession = (IOSession) selectionKey.attachment();
        if (!ioSession.writeResponses()) {
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
                System.err.println("Error while closing selector");
                error.printStackTrace();
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
            System.err.println("Error while closing channel");
            error.printStackTrace();
        }
    }

    @SneakyThrows
    private void stopExecutorService() {
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }
    }
}
