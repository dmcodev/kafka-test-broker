package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

public class BytesCodec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        byte[] bytes = (byte[]) value;
        buffer.putInt(bytes.length);
        buffer.putBytes(bytes);
    }
}
