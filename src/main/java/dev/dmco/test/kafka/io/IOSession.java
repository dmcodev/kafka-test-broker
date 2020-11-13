package dev.dmco.test.kafka.io;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

@RequiredArgsConstructor
class IOSession {

    private static final int MESSAGE_SIZE_BYTES = 4;

    private final ByteBuffer sizeBuffer = ByteBuffer.allocate(MESSAGE_SIZE_BYTES);
    private final Deque<ByteBuffer> writeQueue = new LinkedList<>();

    private final SocketChannel channel;

    private ByteBuffer bodyBuffer;

    @SneakyThrows
    public List<ByteBuffer> readRequests() {
        List<ByteBuffer> requests = new ArrayList<>();
        while (true) {
            if (bodyBuffer == null) {
                if (!readFully(sizeBuffer)) {
                    break;
                }
                int messageSize = sizeBuffer.asIntBuffer().get();
                bodyBuffer = ByteBuffer.allocate(messageSize);
            } else {
                if (!readFully(bodyBuffer)) {
                    break;
                }
                requests.add(bodyBuffer);
                bodyBuffer = null;
            }
        }
        return requests;
    }

    public boolean writeResponse(Collection<ByteBuffer> responseBuffers) {
        enqueueResponse(responseBuffers);
        return writeResponses();
    }

    @SneakyThrows
    public boolean writeResponses() {
        ByteBuffer buffer;
        while ((buffer = writeQueue.peekFirst()) != null) {
            if (channel.write(buffer) == 0) {
                return false;
            }
            if (!buffer.hasRemaining()) {
                writeQueue.removeFirst();
            }
        }
        return true;
    }

    @SneakyThrows
    private boolean readFully(ByteBuffer targetBuffer) {
        int readBytes = channel.read(targetBuffer);
        if (readBytes == -1) {
            throw new ClosedChannelException();
        }
        if (targetBuffer.hasRemaining()) {
            return false;
        }
        targetBuffer.rewind();
        return true;
    }

    private void enqueueResponse(Collection<ByteBuffer> responseBuffers) {
        responseBuffers.forEach(ByteBuffer::rewind);
        writeQueue.addLast(encodeResponseSize(responseBuffers));
        responseBuffers.forEach(writeQueue::addLast);
    }

    private ByteBuffer encodeResponseSize(Collection<ByteBuffer> responseBuffers) {
        int messageSize = responseBuffers.stream()
            .mapToInt(ByteBuffer::remaining)
            .sum();
        ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE_BYTES);
        buffer.putInt(messageSize);
        buffer.rewind();
        return buffer;
    }
}
