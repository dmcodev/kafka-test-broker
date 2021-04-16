package dev.dmcode.test.kafka.io.protocol;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;

class Bytes {

    static byte[] decode(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    static void encode(byte[] value, ResponseBuffer buffer) {
        buffer.putInt(value.length).putBytes(value);
    }
}
