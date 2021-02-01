package dev.dmco.test.kafka.io.protocol;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;

public class VarLong {

    static long decode(ByteBuffer buffer) {
        long value = VarULong.decode(buffer);
        return (value >>> 1) ^ (-(value & 1));
    }

    static void encode(long value, ResponseBuffer buffer) {
        VarULong.encode(toULong(value), buffer);
    }

    static int sizeOf(long value) {
        return VarULong.sizeOf(toULong(value));
    }

    private static long toULong(long value) {
        return (value << 1) ^ (value >> 63);
    }
}
