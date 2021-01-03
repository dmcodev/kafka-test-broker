package dev.dmco.test.kafka.io;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
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

    public List<ByteBuffer> readRequests() throws IOException {
        List<ByteBuffer> requests = new ArrayList<>();
        while (true) {
            if (bodyBuffer == null) {
                if (notReadFully(sizeBuffer)) {
                    break;
                }
                int messageSize = sizeBuffer.asIntBuffer().get();
                bodyBuffer = ByteBuffer.allocate(messageSize);
            } else {
                if (notReadFully(bodyBuffer)) {
                    break;
                }
                requests.add(bodyBuffer);
                bodyBuffer = null;
            }
        }
        return requests;
    }

    public void enqueueResponse(ByteBuffer responseBuffer) {
        responseBuffer.rewind();
        writeQueue.addLast(encodeResponseSize(responseBuffer));
        writeQueue.addLast(responseBuffer);
    }

    @SneakyThrows
    public boolean writeResponses() {
        ByteBuffer buffer;
        while ((buffer = writeQueue.peekFirst()) != null) {
            if (buffer.limit() > 0 && channel.write(buffer) == 0) {
                return false;
            }
            if (!buffer.hasRemaining()) {
                writeQueue.removeFirst();
            }
        }
        return true;
    }

    private boolean notReadFully(ByteBuffer targetBuffer) throws IOException {
        int readBytes = channel.read(targetBuffer);
        if (readBytes == -1) {
            throw new ClosedChannelException();
        }
        if (targetBuffer.hasRemaining()) {
            return true;
        }
        targetBuffer.rewind();
        return false;
    }

    private ByteBuffer encodeResponseSize(ByteBuffer responseBuffer) {
        ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE_BYTES);
        buffer.putInt(responseBuffer.remaining());
        buffer.rewind();
        return buffer;
    }
}
