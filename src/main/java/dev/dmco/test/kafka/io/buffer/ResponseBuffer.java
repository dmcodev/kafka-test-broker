package dev.dmco.test.kafka.io.buffer;

import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ResponseBuffer extends ByteArrayOutputStream {

    private final Map<Integer, ByteBuffer> slots = new HashMap<>();

    public ResponseBuffer() {
        super(128);
    }

    public ResponseBuffer putByte(byte value) {
        write(value);
        return this;
    }

    public ResponseBuffer putShort(short value) {
        write(value >> 8);
        write(value);
        return this;
    }

    public ResponseBuffer putInt(int value) {
        write(value >> 24);
        write(value >> 16);
        write(value >> 8);
        write(value);
        return this;
    }

    public ResponseBuffer putLong(long value) {
        write((int) (value >> 56));
        write((int) (value >> 48));
        write((int) (value >> 40));
        write((int) (value >> 32));
        write((int) (value >> 24));
        write((int) (value >> 16));
        write((int) (value >> 8));
        write((int) value);
        return this;
    }

    @SneakyThrows
    public ResponseBuffer putBytes(byte[] bytes) {
        write(bytes);
        return this;
    }

    public ByteBuffer putIntSlot() {
        return putSlot(Integer.BYTES);
    }

    public byte[] read(int from, int length) {
        byte[] result = new byte[length];
        System.arraycopy(buf, from, result, 0, length);
        return result;
    }

    public int position() {
        return count;
    }

    public ByteBuffer toByteBuffer() {
        slots.forEach((key, value) -> materializeSlot(key, value, buf));
        ByteBuffer buffer = ByteBuffer.wrap(buf);
        buffer.limit(count);
        buf = new byte[0];
        reset();
        return buffer;
    }

    private ByteBuffer putSlot(int size) {
        ByteBuffer slot = ByteBuffer.allocate(size);
        slots.put(count, slot);
        putBytes(new byte[size]);
        return slot;
    }

    private void materializeSlot(int offset, ByteBuffer slot, byte[] bytes) {
        System.arraycopy(slot.array(), 0, bytes, offset, slot.limit());
    }
}
