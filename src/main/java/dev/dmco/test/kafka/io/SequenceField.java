package dev.dmco.test.kafka.io;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class SequenceField extends TypeField {

    private final Class<?> elementType;
    private final int sinceApiVersion;
    private final Function<Object, Object> getter;

    @Override
    public Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        int size = buffer.getInt();
        if (size == 0) {
            return Collections.emptyList();
        }
        return IntStream.range(0, size)
            .mapToObj(i -> decoder.decode(elementType, buffer, apiVersion))
            .collect(Collectors.toList());
    }

    @Override
    public int calculateSize(Object instance, int apiVersion, IOEncoder encoder) {
        Collection<?> fieldValue = (Collection<?>) getter.apply(instance);
        int elementsSize = fieldValue.stream()
            .mapToInt(element -> encoder.calculateSize(element, apiVersion))
            .sum();
        return 4 + elementsSize;
    }

    @Override
    public void encode(Object instance, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        Collection<?> fieldValue = Optional.ofNullable(getter.apply(instance))
            .map(Collection.class::cast)
            .orElseGet(Collections::emptyList);
        buffer.putInt(fieldValue.size());
        fieldValue.forEach(element -> encoder.encode(element, apiVersion, buffer));
    }
}
