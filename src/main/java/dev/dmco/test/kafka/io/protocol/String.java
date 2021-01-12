package dev.dmco.test.kafka.io.protocol;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class String {

    static java.lang.String decode(ByteBuffer buffer) {
        int length = buffer.getShort();
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new java.lang.String(chars, StandardCharsets.UTF_8);
    }

    static void encode(java.lang.String value, ResponseBuffer buffer) {
        byte[] chars = value.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) chars.length)
            .putBytes(chars);
    }
}
