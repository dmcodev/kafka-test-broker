package dev.dmco.test.kafka.io.struct;

import dev.dmco.test.kafka.io.IODecoder;
import dev.dmco.test.kafka.io.IOEncoder;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

abstract class SequenceField extends FieldHandle {

    public SequenceField(Field field) {
        super(field);
        if (!Collection.class.isAssignableFrom(field.getType())) {
            throw new IllegalArgumentException("Field " + field + " type must be a subtype of " + Collection.class.getName());
        }
    }

    @Override
    public Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        int size = buffer.getInt();
        if (size <= 0) {
            return Collections.emptyList();
        }
        return IntStream.range(0, size)
            .mapToObj(i -> decodeElement(buffer, apiVersion, decoder))
            .collect(Collectors.toList());
    }

    @Override
    public int encodedSize(Object struct, int apiVersion, IOEncoder encoder) {
        Collection<?> sequence = sequenceValue(struct);
        int elementsSize = sequence.stream()
            .mapToInt(element -> encodedElementSize(element, apiVersion, encoder))
            .sum();
        return FieldType.INT32_SIZE + elementsSize;
    }

    @Override
    public void encode(Object struct, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        Collection<?> sequence = sequenceValue(struct);
        buffer.putInt(sequence.size());
        sequence.forEach(element -> encodeElement(element, apiVersion, buffer, encoder));
    }

    private Collection<?> sequenceValue(Object struct) {
        return Optional.ofNullable(valueFrom(struct))
            .map(Collection.class::cast)
            .orElseGet(Collections::emptyList);
    }

    protected abstract Object decodeElement(ByteBuffer buffer, int apiVersion, IODecoder decoder);

    protected abstract int encodedElementSize(Object element, int apiVersion, IOEncoder encoder);

    protected abstract void encodeElement(Object element, int apiVersion, ByteBuffer buffer, IOEncoder encoder);
}
