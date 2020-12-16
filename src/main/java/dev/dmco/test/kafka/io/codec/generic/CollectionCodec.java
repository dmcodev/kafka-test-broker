package dev.dmco.test.kafka.io.codec.generic;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.CodecRegistry;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CollectionCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Collection.class, Type.of(Object.class)));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        int size = buffer.getInt();
        if (size <= 0) {
            return Collections.emptyList();
        }
        Type elementType = targetType.typeParameters().get(0);
        Codec elementCodec = CodecRegistry.getCodec(elementType);
        return IntStream.range(0, size)
            .mapToObj(i -> elementCodec.decode(buffer, elementType, context))
            .collect(Collectors.toList());
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Collection<?> collection = Optional.ofNullable(value)
            .map(Collection.class::cast)
            .orElseGet(Collections::emptyList);
        buffer.putInt(collection.size());
        Type elementType = valueType.typeParameters().get(0);
        Codec elementCodec = CodecRegistry.getCodec(elementType);
        collection.forEach(element -> encodeElement(element, elementType, elementCodec, buffer, context));
    }

    private void encodeElement(Object element, Type elementType, Codec elementCodec, ResponseBuffer buffer, CodecContext context) {
        if (element != null) {
            elementCodec.encode(element, elementType, buffer, context);
        } else {
            elementCodec.encodeNull(elementType, buffer, context);
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Collections.emptyList(), valueType, buffer, context);
    }
}
