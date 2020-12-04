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
        current = ByteBuffer.allocate(chunkSize);
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
        int written = Math.min(bytes.length, current.remaining());
        current.put(bytes, 0, written);
        int remaining = bytes.length - written;
        requireBytes(remaining);
        current.put(bytes, written, remaining);
        return this;
    }

    public ResponseBuffer putBuffers(Collection<ByteBuffer> bufferList) {
        allocate();
        buffers.addAll(bufferList);
        return this;
    }

    public int size() {
        return buffers.stream().mapToInt(Buffer::limit).sum()
            + current.position();
    }

    public List<ByteBuffer> collect() {
        allocate();
        List<ByteBuffer> result = new ArrayList<>(buffers);
        buffers.clear();
        return result;
    }

    private void requireBytes(int requiredBytes) {
        if (current.remaining() < requiredBytes) {
            allocate(requiredBytes);
        }
    }

    private void allocate() {
        allocate(chunkSize);
    }

    private void allocate(int requiredBytes) {
        enqueue();
        int allocationSize = Math.max(requiredBytes, chunkSize);
        current = ByteBuffer.allocate(allocationSize);
    }

    private void enqueue() {
        current.limit(current.position());
        buffers.addLast(current);
    }
}
