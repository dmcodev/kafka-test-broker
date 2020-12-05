package dev.dmco.test.kafka.io.codec.generic;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.context.rules.CodecRule;
import dev.dmco.test.kafka.io.codec.context.rules.CodecRulesAware;
import dev.dmco.test.kafka.io.codec.context.rules.binding.CodecRuleBindings;
import dev.dmco.test.kafka.io.codec.registry.CodecRegistry;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;
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

import static dev.dmco.test.kafka.io.codec.registry.TypeKey.key;

public class ObjectCodec implements Codec {

    private final Map<TypeKey, TypeMetadata> metadata = new HashMap<>();

    @Override
    public Stream<TypeKey> handledTypes() {
        return Stream.of(
            key(Object.class)
        );
    }

    @Override
    @SneakyThrows
    public Object decode(ByteBuffer buffer, CodecContext context) {
        TypeMetadata metadata = getMetadata(context);
        CodecContext objectContext = metadata.createContextFromRules(context);
        Constructor<?> constructor = metadata.constructor();
        Collection<TypeProperty> properties = metadata.properties();
        List<Object> constructorArguments = new ArrayList<>(constructor.getParameterCount());
        for (TypeProperty property : properties) {
            CodecContext propertyContext = property.createContextFromRules(objectContext)
                .set(ContextProperty.CURRENT_TYPE_KEY, property.typeKey());
            if (property.isExcluded(propertyContext)) {
                constructorArguments.add(property.getEmptyValue());
            } else {
                Object fieldValue = property.selectCodec()
                    .decode(buffer, propertyContext);
                constructorArguments.add(fieldValue);
            }
        }
        return constructor.newInstance(constructorArguments.toArray());
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        TypeMetadata metadata = getMetadata(context);
        CodecContext objectContext = metadata.createContextFromRules(context);
        Collection<TypeProperty> properties = metadata.properties();
        for (TypeProperty property : properties) {
            CodecContext propertyContext = property.createContextFromRules(objectContext)
                .set(ContextProperty.CURRENT_TYPE_KEY, property.typeKey());
            if (!property.isExcluded(propertyContext)) {
                Object propertyValue = property.getValue(value);
                property.selectCodec()
                    .encode(propertyValue, buffer, propertyContext);
            }
        }
    }

    private TypeMetadata getMetadata(CodecContext context) {
        return metadata.computeIfAbsent(context.get(ContextProperty.CURRENT_TYPE_KEY), key -> new TypeMetadata(key.rawType()));
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
        private final Class<?> propertyType;
        private final TypeKey typeKey;
        private final Collection<CodecRule> codecRules;

        TypeProperty(Field field) {
            field.setAccessible(true);
            getter = field;
            propertyType = field.getType();
            typeKey = TypeKey.key(field);
            codecRules = Arrays.stream(field.getAnnotations())
                .map(CodecRuleBindings::createRules)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        }

        @SneakyThrows
        Object getValue(Object instance) {
            return getter.get(instance);
        }

        Codec selectCodec() {
            return CodecRegistry.getCodec(typeKey);
        }

        boolean isExcluded(CodecContext context) {
            return context.getOrDefault(ContextProperty.EXCLUDE_FIELD, false);
        }

        Object getEmptyValue() {
            if (Object.class.isAssignableFrom(propertyType)) {
                return null;
            } else {
                if (byte.class == propertyType) {
                    return (byte) 0;
                } else if (short.class == propertyType) {
                    return (short) 0;
                } else if (int.class == propertyType) {
                    return 0;
                } else if (float.class == propertyType) {
                    return 0.0f;
                } else if (double.class == propertyType) {
                    return 0.0;
                } else if (boolean.class == propertyType) {
                    return false;
                } else {
                    throw new IllegalArgumentException("Could not produce empty value for field type: " + propertyType);
                }
            }
        }
    }
}
