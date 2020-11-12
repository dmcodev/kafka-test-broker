package dev.dmco.test.kafka.io;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.function.Function;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class StructField extends TypeField {

    private final Class<?> type;
    private final int sinceApiVersion;
    private final Function<Object, Object> getter;

    @Override
    public Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        return decoder.decode(type, buffer, apiVersion);
    }

    @Override
    public int calculateSize(Object instance, int apiVersion, IOEncoder encoder) {
        Object fieldValue = getter.apply(instance);
        return encoder.calculateSize(fieldValue, apiVersion);
    }

    @Override
    public void encode(Object instance, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        Object fieldValue = getter.apply(instance);
        encoder.encode(fieldValue, apiVersion, buffer);
    }
}
