package dev.dmco.test.kafka.io.codec;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

public interface Codec {

    Object decode(ByteBuffer buffer, CodecContext context);

    void encode(Object value, ResponseBuffer buffer, CodecContext context);

    default Codec compacted() {
        throw new IllegalStateException("Codec has not compacted version");
    }
}
