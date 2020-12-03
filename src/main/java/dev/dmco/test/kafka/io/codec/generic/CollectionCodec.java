package dev.dmco.test.kafka.io.codec.generic;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.CodecRegistry;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class CollectionCodec implements Codec {

    private final Codec elementCodec;

    public static CollectionCodec from(TypeKey typeKey) {
        List<TypeKey> typeParameters = typeKey.typeParameters();
        if (typeParameters.size() != 1) {
            throw new IllegalArgumentException("Single type parameter expected, was: " + typeParameters);
        }
        TypeKey elementTypeKey = typeParameters.get(0);
        Codec elementCodec = CodecRegistry.getCodec(elementTypeKey);
        return new CollectionCodec(elementCodec);
    }


    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int size = buffer.getInt();
        if (size <= 0) {
            return Collections.emptyList();
        }
        return IntStream.range(0, size)
            .mapToObj(i -> elementCodec.decode(buffer, context))
            .collect(Collectors.toList());
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Collection<?> collection = Optional.ofNullable(value)
            .map(Collection.class::cast)
            .orElseGet(Collections::emptyList);
        buffer.putInt(collection.size());
        collection.forEach(element -> elementCodec.encode(element, buffer, context));
    }
}
