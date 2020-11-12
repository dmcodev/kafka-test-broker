package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.messages.meta.PrimitiveType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class PrimitiveField extends TypeField {

    private final PrimitiveType type;
    private final int sinceApiVersion;
    private final Function<Object, Object> getter;

    @Override
    public Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        switch (type) {
            case INT8:
                return buffer.get();
            case INT16:
                return buffer.getShort();
            case INT32:
                return buffer.getInt();
        }
        return null;
    }

    @Override
    public int calculateSize(Object instance, int apiVersion, IOEncoder encoder) {
        switch (type) {
            case INT8:
                return 1;
            case INT16:
                return 2;
            case INT32:
                return 4;
        }
        return 0;
    }

    @Override
    public void encode(Object instance, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        Object fieldValue = getter.apply(instance);
        switch (type) {
            case INT8:
                buffer.put(
                    Optional.ofNullable(fieldValue)
                        .map(Byte.class::cast)
                        .orElse((byte) 0)
                );
                break;
            case INT16:
                buffer.putShort(
                    Optional.ofNullable(fieldValue)
                        .map(Short.class::cast)
                        .orElse((short) 0)
                );
                break;
            case INT32:
                buffer.putInt(
                    Optional.ofNullable(fieldValue)
                        .map(Integer.class::cast)
                        .orElse(0)
                );
                break;
        }
    }
}
