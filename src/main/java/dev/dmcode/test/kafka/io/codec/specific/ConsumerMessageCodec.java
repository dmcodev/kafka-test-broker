package dev.dmcode.test.kafka.io.codec.specific;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.codec.Codec;
import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmcode.test.kafka.io.codec.registry.Type;
import dev.dmcode.test.kafka.messages.consumer.ConsumerMessage;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

@RequiredArgsConstructor
abstract class ConsumerMessageCodec implements Codec {

    private final Class<? extends ConsumerMessage> type;

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(type));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        buffer.getInt();
        buffer.mark();
        int version = buffer.getShort();
        buffer.reset();
        CodecContext objectContext = context.withVersion(version);
        return ObjectCodec.decode(buffer, type, objectContext);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        ConsumerMessage versioned = (ConsumerMessage) value;
        CodecContext objectContext = context.withVersion(versioned.version());
        ByteBuffer sizeSlot = buffer.putIntSlot();
        int startPosition = buffer.position();
        ObjectCodec.encode(value, buffer, objectContext);
        int objectSize = buffer.position() - startPosition;
        sizeSlot.putInt(objectSize);
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        throw new NullPointerException("Could not encode null " + type.getSimpleName());
    }
}
