package dev.dmco.test.kafka;

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

class IOEventLoop implements AutoCloseable {

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final ExecutorService executorService;

    private final AtomicBoolean stopped = new AtomicBoolean();

    @SneakyThrows
    IOEventLoop(TestKafkaBrokerConfig config) {
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
                numberOfSelectedKeys = selector.select();
            } catch (Exception ex) {
                System.err.println("Selector error");
                ex.printStackTrace();
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
                        } catch (Exception ex) {
                            System.err.println("Connection error");
                            ex.printStackTrace();
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
        clientChannel.register(selector, SelectionKey.OP_READ);
    }

    @SneakyThrows
    private void readFromClient(SelectionKey selectionKey) {
        SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        clientChannel.read(byteBuffer);

        // TODO: read request
        System.out.println("...");
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
