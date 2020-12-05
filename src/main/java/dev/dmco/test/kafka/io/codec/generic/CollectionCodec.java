package dev.dmco.test.kafka.io.codec.generic;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.registry.CodecRegistry;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.codec.registry.TypeKey.key;

public class CollectionCodec implements Codec {

    @Override
    public Stream<TypeKey> handledTypes() {
        return Stream.of(
            key(Collection.class, key(Object.class))
        );
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int size = buffer.getInt();
        if (size <= 0) {
            return Collections.emptyList();
        }
        TypeKey elementTypeKey = context.get(ContextProperty.CURRENT_TYPE_KEY)
            .typeParameters().get(0);
        Codec elementCodec = CodecRegistry.getCodec(elementTypeKey);
        CodecContext elementContext = context.set(ContextProperty.CURRENT_TYPE_KEY, elementTypeKey);
        return IntStream.range(0, size)
            .mapToObj(i -> elementCodec.decode(buffer, elementContext))
            .collect(Collectors.toList());
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Collection<?> collection = Optional.ofNullable(value)
            .map(Collection.class::cast)
            .orElseGet(Collections::emptyList);
        buffer.putInt(collection.size());
        TypeKey elementTypeKey = context.get(ContextProperty.CURRENT_TYPE_KEY).typeParameters().get(0);
        Codec elementCodec = CodecRegistry.getCodec(elementTypeKey);
        CodecContext elementContext = context.set(ContextProperty.CURRENT_TYPE_KEY, elementTypeKey);
        collection.forEach(element -> elementCodec.encode(element, buffer, elementContext));
    }
}
