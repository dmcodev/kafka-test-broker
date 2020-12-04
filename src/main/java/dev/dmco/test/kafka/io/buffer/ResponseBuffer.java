package dev.dmco.test.kafka.io.buffer;

import lombok.experimental.Accessors;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

@Accessors(fluent = true)
public class ResponseBuffer {

    private final LinkedList<ByteBuffer> buffers = new LinkedList<>();

    private final int chunkSize;
    private ByteBuffer current;

    public ResponseBuffer() {
        this(32);
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
        current.put(value);
        return this;
    }

    public ResponseBuffer putShort(short value) {
        requireBytes(2);
        current.putShort(value);
        return this;
    }

    public ResponseBuffer putInt(int value) {
        requireBytes(4);
        current.putInt(value);
        return this;
    }

    public ResponseBuffer putLong(long value) {
        requireBytes(8);
        current.putLong(value);
        return this;
    }

    public ResponseBuffer putBytes(byte[] bytes) {
        if (current.remaining() >= bytes.length) {
            current.put(bytes);
        } else {
            int written = current.remaining();
            current.put(bytes, 0, written);
            int remaining = bytes.length - written;
            requireBytes(remaining);
            current.put(bytes, written, remaining);
        }
        return this;
    }

    public ResponseBuffer putBuffers(Collection<ByteBuffer> bufferList) {
        enqueue();
        current = null;
        buffers.addAll(bufferList);
        return this;
    }

    public int size() {
        return buffers.stream().mapToInt(Buffer::limit).sum()
            + ((current != null) ? current.position() : 0);
    }

    public List<ByteBuffer> collect() {
        enqueue();
        List<ByteBuffer> result = new ArrayList<>(buffers);
        buffers.clear();
        current = null;
        return result;
    }

    private void requireBytes(int requiredBytes) {
        if (current == null || current.remaining() < requiredBytes) {
            allocate(requiredBytes);
        }
    }

    private void allocate(int requiredBytes) {
        enqueue();
        int allocationSize = Math.max(requiredBytes, chunkSize);
        current = ByteBuffer.allocate(allocationSize);
    }

    private void enqueue() {
        if (current != null) {
            current.limit(current.position());
            buffers.addLast(current);
        }
    }
}
