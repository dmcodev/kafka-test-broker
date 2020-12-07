package dev.dmco.test.kafka.io.codec.generic;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.registry.CodecRegistry;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;
import dev.dmco.test.kafka.messages.Versioned;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.codec.registry.TypeKey.key;

@RequiredArgsConstructor
public abstract class VersionedBytesCodec implements Codec {

    private final Class<? extends Versioned> objectType;

    @Override
    public Stream<TypeKey> handledTypes() {
        return Stream.of(
            key(objectType)
        );
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.getInt();
        buffer.mark();
        int version = buffer.getShort();
        buffer.reset();
        CodecContext objectContext = context
            .set(ContextProperty.VERSION, version)
            .set(ContextProperty.CURRENT_TYPE_KEY, key(objectType));
        return CodecRegistry.getCodec(ObjectCodec.class)
            .decode(buffer, objectContext);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Versioned versioned = (Versioned) value;
        CodecContext objectContext = context
            .set(ContextProperty.VERSION, (int) versioned.version())
            .set(ContextProperty.CURRENT_TYPE_KEY, key(objectType));
        ResponseBuffer objectBuffer = new ResponseBuffer();
        CodecRegistry.getCodec(ObjectCodec.class)
            .encode(value, objectBuffer, objectContext);
        buffer.putInt(objectBuffer.size());
        List<ByteBuffer> objectBuffers = objectBuffer.collect();
        buffer.putBuffers(objectBuffers);
    }
}
