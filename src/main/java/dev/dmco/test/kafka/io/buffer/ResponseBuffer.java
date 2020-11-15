package dev.dmco.test.kafka.io.buffer;

import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

@Accessors(fluent = true)
public class ResponseBuffer {

    private final LinkedList<ByteBuffer> buffers = new LinkedList<>();

    private final int chunkSize;
    private ByteBuffer buffer;

    public ResponseBuffer() {
        this(1);
    }

    public ResponseBuffer(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Invalid chunk size: " + chunkSize);
        }
        this.chunkSize = chunkSize;
        allocate(chunkSize);
    }

    public ResponseBuffer putByte(byte value) {
        requireBytes(1);
        buffer.put(value);
        return this;
    }

    public ResponseBuffer putShort(short value) {
        requireBytes(2);
        buffer.putShort(value);
        return this;
    }

    public ResponseBuffer putInt(int value) {
        requireBytes(4);
        buffer.putInt(value);
        return this;
    }

    public ResponseBuffer putLong(long value) {
        requireBytes(8);
        buffer.putLong(value);
        return this;
    }

    public ResponseBuffer putBytes(byte[] bytes) {
        if (buffer.remaining() >= bytes.length) {
            buffer.put(bytes);
        } else {
            int written = buffer.remaining();
            buffer.put(bytes, 0, written);
            requireBytes(bytes.length - written);
            buffer.put(bytes, written, bytes.length);
        }
        return this;
    }

    public List<ByteBuffer> buffers() {
        enqueueBuffer();
        return buffers;
    }

    private void requireBytes(int requiredBytes) {
        if (buffer.remaining() < requiredBytes) {
            allocate(requiredBytes);
        }
    }

    private void allocate(int requiredBytes) {
        enqueueBuffer();
        int allocationSize = Math.max(requiredBytes, chunkSize);
        buffer = ByteBuffer.allocate(allocationSize);
    }

    private void enqueueBuffer() {
        if (buffer != null) {
            buffer.limit(buffer.position());
            buffers.addLast(buffer);
        }
    }
}
