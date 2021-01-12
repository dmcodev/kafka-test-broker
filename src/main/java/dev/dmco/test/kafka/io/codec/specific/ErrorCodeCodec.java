package dev.dmco.test.kafka.io.codec.specific;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.ErrorCode;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class ErrorCodeCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(ErrorCode.class));
    }

    @Override
    public ErrorCode decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        return ErrorCode.of(buffer.getShort());
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        buffer.putShort(((ErrorCode) value).value());
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(ErrorCode.NO_ERROR, valueType, buffer, context);
    }
}
