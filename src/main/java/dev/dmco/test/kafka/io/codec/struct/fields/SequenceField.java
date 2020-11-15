package dev.dmco.test.kafka.io.codec.struct.fields;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.struct.StructEntry;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class SequenceField extends StructEntry {

    public SequenceField(Field field) {
        super(field);
        if (!Collection.class.isAssignableFrom(field.getType())) {
            throw new IllegalArgumentException("Field " + field + " type must be a subtype of " + Collection.class.getName());
        }
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int size = buffer.getInt();
        if (size <= 0) {
            return Collections.emptyList();
        }
        return IntStream.range(0, size)
            .mapToObj(i -> decodeElement(buffer, context))
            .collect(Collectors.toList());
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Collection<?> sequence = sequenceValue(value);
        buffer.putInt(sequence.size());
        sequence.forEach(element -> encodeElement(element, buffer, context));
    }

    private Collection<?> sequenceValue(Object value) {
        return Optional.ofNullable(value)
            .map(Collection.class::cast)
            .orElseGet(Collections::emptyList);
    }

    protected abstract Object decodeElement(ByteBuffer buffer, CodecContext context);

    protected abstract void encodeElement(Object value, ResponseBuffer buffer, CodecContext context);
}
