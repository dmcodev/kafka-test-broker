package dev.dmco.test.kafka.io.protocol;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;

class Bytes {

    static byte[] decode(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    static void encode(Object value, ResponseBuffer buffer) {
        byte[] bytes = (byte[]) value;
        buffer.putInt(bytes.length)
            .putBytes(bytes);
    }
}
