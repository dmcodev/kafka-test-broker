package dev.dmco.test.kafka.io.codec.generic;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.context.rules.CodecRule;
import dev.dmco.test.kafka.io.codec.context.rules.CodecRulesAware;
import dev.dmco.test.kafka.io.codec.context.rules.binding.CodecRuleBindings;
import dev.dmco.test.kafka.io.codec.registry.CodecRegistry;
import dev.dmco.test.kafka.io.codec.registry.Type;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ObjectCodec implements Codec {

    private static final Map<Class<?>, TypeMetadata> METADATA = new HashMap<>();

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Object.class));
    }

    @Override
    @SneakyThrows
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        return decode(buffer, targetType.raw(), context);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(value, buffer, context);
    }

    @SneakyThrows
    public static Object decode(ByteBuffer buffer, Class<?> targetType, CodecContext context) {
        TypeMetadata metadata = METADATA.computeIfAbsent(targetType, TypeMetadata::new);
        CodecContext objectContext = metadata.createContextFromRules(context);
        Constructor<?> constructor = metadata.constructor();
        Collection<TypeProperty> properties = metadata.properties();
        List<Object> constructorArguments = new ArrayList<>(constructor.getParameterCount());
        for (TypeProperty property : properties) {
            constructorArguments.add(property.decode(buffer, objectContext));
        }
        return constructor.newInstance(constructorArguments.toArray());
    }

    public static void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        TypeMetadata metadata = METADATA.computeIfAbsent(value.getClass(), TypeMetadata::new);
        CodecContext objectContext = metadata.createContextFromRules(context);
        Collection<TypeProperty> properties = metadata.properties();
        for (TypeProperty property : properties) {
            property.encode(value, buffer, objectContext);
        }
    }

    @Getter
    @Accessors(fluent = true)
    private static class TypeMetadata implements CodecRulesAware {

        private final Collection<TypeProperty> properties;
        private final Constructor<?> constructor;
        private final Collection<CodecRule> codecRules;

        TypeMetadata(Class<?> type) {
            properties = Arrays.stream(type.getDeclaredFields())
                .map(TypeProperty::new)
                .collect(Collectors.toList());
            constructor = type.getConstructors()[0];
            codecRules = Arrays.stream(type.getAnnotations())
                .map(CodecRuleBindings::createRules)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        }
    }

    @Getter
    @Accessors(fluent = true)
    private static class TypeProperty implements CodecRulesAware {

        private final Field getter;
        private final Class<?> rawType;
        private final Type type;
        private final Collection<CodecRule> codecRules;

        TypeProperty(Field field) {
            field.setAccessible(true);
            getter = field;
            rawType = field.getType();
            type = Type.of(field);
            codecRules = Arrays.stream(field.getAnnotations())
                .map(CodecRuleBindings::createRules)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        }

        Object decode(ByteBuffer buffer, CodecContext objectContext) {
            CodecContext propertyContext = createContextFromRules(objectContext);
            if (isIncluded(propertyContext)) {
                return CodecRegistry.getCodec(type)
                    .decode(buffer, type, propertyContext);
            } else {
                return produceEmptyValue();
            }
        }

        @SneakyThrows
        void encode(Object instance, ResponseBuffer buffer, CodecContext objectContext) {
            CodecContext propertyContext = createContextFromRules(objectContext);
            if (isIncluded(propertyContext)) {
                Object value = getter.get(instance);
                CodecRegistry.getCodec(type)
                    .encode(value, type, buffer, propertyContext);
            }
        }

        private boolean isIncluded(CodecContext context) {
            return !context.getOrDefault(ContextProperty.VERSION_MISMATCH, false);
        }

        private Object produceEmptyValue() {
            if (Object.class.isAssignableFrom(rawType)) {
                return null;
            } else {
                if (byte.class == rawType) {
                    return (byte) 0;
                } else if (short.class == rawType) {
                    return (short) 0;
                } else if (int.class == rawType) {
                    return 0;
                } else if (float.class == rawType) {
                    return 0.0f;
                } else if (double.class == rawType) {
                    return 0.0;
                } else if (boolean.class == rawType) {
                    return false;
                } else {
                    throw new IllegalArgumentException("Could not produce empty value for field type: " + rawType);
                }
            }
        }
    }
}
