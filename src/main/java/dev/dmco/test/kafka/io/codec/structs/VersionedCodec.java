package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.Versioned;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

@RequiredArgsConstructor
abstract class VersionedCodec implements Codec {

    private final Class<? extends Versioned> type;

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
        CodecContext objectContext = context.set(ContextProperty.VERSION, version);
        return ObjectCodec.decode(buffer, type, objectContext);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Versioned versioned = (Versioned) value;
        CodecContext objectContext = context.set(ContextProperty.VERSION, (int) versioned.version());
        ByteBuffer sizeSlot = buffer.putIntSlot();
        int startPosition = buffer.position();
        ObjectCodec.encode(value, buffer, objectContext);
        int objectSize = buffer.position() - startPosition;
        sizeSlot.putInt(objectSize);
    }
}
