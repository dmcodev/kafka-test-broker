package dev.dmco.test.kafka.io.buffer;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class ResponseBuffer {

    private final LinkedList<ByteBuffer> committedBuffers = new LinkedList<>();

    private final int defaultAllocationSize;
    private ByteBuffer currentBuffer;

    public ResponseBuffer() {
        this(32);
    }

    public ResponseBuffer(int defaultAllocationSize) {
        this.defaultAllocationSize = defaultAllocationSize;
        currentBuffer = ByteBuffer.allocate(defaultAllocationSize);
    }

    public ResponseBuffer putByte(byte value) {
        requireBytes(1);
        currentBuffer.put(value);
        return this;
    }

    public ResponseBuffer putShort(short value) {
        requireBytes(2);
        currentBuffer.putShort(value);
        return this;
    }

    public ResponseBuffer putInt(int value) {
        requireBytes(4);
        currentBuffer.putInt(value);
        return this;
    }

    public ResponseBuffer putLong(long value) {
        requireBytes(8);
        currentBuffer.putLong(value);
        return this;
    }

    public ResponseBuffer putBytes(byte[] bytes) {
        int written = Math.min(bytes.length, currentBuffer.remaining());
        currentBuffer.put(bytes, 0, written);
        int remaining = bytes.length - written;
        requireBytes(remaining);
        currentBuffer.put(bytes, written, remaining);
        return this;
    }

    public ResponseBuffer putBuffers(Collection<ByteBuffer> bufferList) {
        commit();
        committedBuffers.addAll(bufferList);
        return this;
    }

    public int size() {
        return committedBuffers.stream().mapToInt(Buffer::limit).sum() + currentBuffer.position();
    }

    public List<ByteBuffer> collect() {
        commit();
        List<ByteBuffer> result = new ArrayList<>(committedBuffers);
        committedBuffers.clear();
        return result;
    }

    private void requireBytes(int requiredBytes) {
        if (currentBuffer.remaining() < requiredBytes) {
            commitAndAllocate(requiredBytes);
        }
    }

    private void commit() {
        commitAndAllocate(defaultAllocationSize);
    }

    private void commitAndAllocate(int requiredBytes) {
        commitCurrentBuffer();
        int allocationSize = Math.max(requiredBytes, defaultAllocationSize);
        currentBuffer = ByteBuffer.allocate(allocationSize);
    }

    private void commitCurrentBuffer() {
        currentBuffer.limit(currentBuffer.position());
        committedBuffers.addLast(currentBuffer);
    }
}
