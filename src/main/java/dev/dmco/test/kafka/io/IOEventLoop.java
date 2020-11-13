package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.SneakyThrows;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class IOEventLoop implements AutoCloseable {

    private static final int SELECT_TIMEOUT = 1000;

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final ExecutorService executorService;

    private final Function<RequestMessage, ResponseMessage> requestHandler;

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final IOEncoder encoder = new IOEncoder();
    private final IODecoder decoder = new IODecoder();

    @SneakyThrows
    public IOEventLoop(
        InetSocketAddress bindAddress,
        Function<RequestMessage, ResponseMessage> requestHandler
    ) {
        this.requestHandler = requestHandler;
        try {
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            serverChannel.bind(bindAddress);
            executorService = Executors.newSingleThreadExecutor();
            executorService.execute(this::eventLoop);
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
                        } catch (Exception error) {
                            System.err.println("Connection error");
                            error.printStackTrace();
                            closeSelectionKey(selectionKey);
                        }
                    }
                    selectedKeys.remove();
                }
            }
        }
    }

    @SneakyThrows
    private void onClientConnectionInitializing() {
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        IOSession ioSession = new IOSession(clientChannel);
        clientChannel.register(selector, SelectionKey.OP_READ, ioSession);
    }

    private void onClientConnectionReadable(SelectionKey selectionKey) {
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
        ResponseMessage response = requestHandler.apply(request);
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

    private void stopExecutorService() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}
