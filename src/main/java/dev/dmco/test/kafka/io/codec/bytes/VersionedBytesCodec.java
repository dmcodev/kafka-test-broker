package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.messages.Versioned;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.List;

@RequiredArgsConstructor
public abstract class VersionedBytesCodec implements Codec {

    private final Codec delegate;

    public VersionedBytesCodec(Class<? extends Versioned> objectType) {
        delegate = new ObjectCodec(objectType);
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.getInt();
        buffer.mark();
        int version = buffer.getShort();
        buffer.reset();
        CodecContext objectContext = context.set(ContextProperty.VERSION, version);
        return delegate.decode(buffer, objectContext);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Versioned versioned = (Versioned) value;
        CodecContext objectContext = context.set(ContextProperty.VERSION, (int) versioned.version());
        ResponseBuffer objectBuffer = new ResponseBuffer();
        delegate.encode(value, objectBuffer, objectContext);
        buffer.putInt(objectBuffer.size());
        List<ByteBuffer> objectBuffers = objectBuffer.collect();
        buffer.putBuffers(objectBuffers);
    }
}
