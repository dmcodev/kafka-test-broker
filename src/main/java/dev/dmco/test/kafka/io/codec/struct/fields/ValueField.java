package dev.dmco.test.kafka.io.codec.struct.fields;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.struct.StructEntry;
import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.TypeOverride;
import dev.dmco.test.kafka.messages.meta.TypeOverrides;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ValueField extends StructEntry {

    private final Map<Integer, ValueType> typeOverridesMapping = new HashMap<>();
    private final List<TypeOverride> typeOverrides;

    private final ValueType baseType;

    public ValueField(Field field, ValueType baseType) {
        super(field);
        this.baseType = baseType;
        typeOverrides = getTypeOverrides(field);
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return getEffectiveType(context.apiVersion())
            .decode(buffer, context);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        getEffectiveType(context.apiVersion())
            .encode(value, buffer, context);
    }

    private ValueType getEffectiveType(int apiVersion) {
        return typeOverridesMapping.computeIfAbsent(apiVersion, this::calculateTypeOverride);
    }

    private ValueType calculateTypeOverride(int apiVersion) {
        return typeOverrides.stream()
            .sorted(Comparator.comparingInt(TypeOverride::sinceApiVersion).reversed())
            .filter(mapping -> apiVersion >= mapping.sinceApiVersion())
            .map(TypeOverride::value)
            .findFirst()
            .orElse(baseType);
    }

    private List<TypeOverride> getTypeOverrides(Field field) {
        return Optional.ofNullable(field.getAnnotation(TypeOverride.class))
            .map(Collections::singletonList)
            .orElseGet(() ->
                Optional.ofNullable(field.getAnnotation(TypeOverrides.class))
                    .map(TypeOverrides::value)
                    .map(mappings -> Arrays.asList(mappings))
                    .orElseGet(Collections::emptyList)
            );
    }
}
