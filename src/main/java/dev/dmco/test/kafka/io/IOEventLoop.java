package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.TestKafkaBrokerConfig;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.state.BrokerState;
import lombok.SneakyThrows;

import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class IOEventLoop implements AutoCloseable {

    private static final int SELECT_TIMEOUT = 1000;

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final ExecutorService executorService;
    private final BrokerState brokerState;

    private final AtomicBoolean stopped = new AtomicBoolean();

    @SneakyThrows
    public IOEventLoop(
        TestKafkaBrokerConfig config,
        BrokerState brokerState
    ) {
        this.brokerState = brokerState;
        try {
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            serverChannel.bind(new InetSocketAddress(config.host(), config.port()));
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
                                connectClient();
                            }
                            if (selectionKey.isReadable()) {
                                readFromClient(selectionKey);
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
    private void connectClient() {
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        IOSession ioSession = new IOSession(clientChannel);
        clientChannel.register(selector, SelectionKey.OP_READ, ioSession);
    }

    @SneakyThrows
    private void readFromClient(SelectionKey selectionKey) {
        IOSession ioSession = (IOSession) selectionKey.attachment();
        ioSession.readRequests()
            .stream()
            .map(IODecoder::decode)
            .forEach(request -> handleRequest(request, ioSession, selectionKey));
    }

    private void handleRequest(RequestMessage request, IOSession ioSession, SelectionKey selectionKey) {
        ResponseMessage response = brokerState.handlersRegistry()
            .selectHandler(request)
            .handle(request, brokerState);
        ResponseBuffer responseBuffer = IOEncoder.encode(response, request.header());
        if (!ioSession.writeResponse(responseBuffer)) {
            selectionKey.interestOps(selectionKey.interestOps() & SelectionKey.OP_WRITE);
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
