package dev.dmco.test.kafka.io.codec.struct;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StructCodec {

    private final Map<Class<?>, Struct> metadata = new HashMap<>();

    @SneakyThrows
    public Object decode(Class<?> targetType, ByteBuffer buffer, CodecContext context) {
        Struct metadata = metadataFor(targetType);
        Constructor<?> constructor = metadata.constructor();
        Collection<StructEntry> fields = metadata.fields();
        List<Object> constructorArguments = new ArrayList<>(constructor.getParameterCount());
        int apiVersion = context.apiVersion();
        for (StructEntry field : fields) {
            if (field.presentInApiVersion(apiVersion)) {
                int overriddenApiVersion = field.overrideApiVersion(apiVersion);
                CodecContext fieldCodecContext = (overriddenApiVersion != apiVersion)
                    ? context.withApiVersion(overriddenApiVersion)
                    : context;
                Object fieldValue = field.decode(buffer, fieldCodecContext);
                constructorArguments.add(fieldValue);
            } else {
                constructorArguments.add(field.emptyValue());
            }
        }
        return constructor.newInstance(constructorArguments.toArray());
    }

    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Collection<StructEntry> fields = metadataFor(value.getClass())
            .fields().stream()
            .filter(field -> field.presentInApiVersion(context.apiVersion()))
            .collect(Collectors.toList());
        int apiVersion = context.apiVersion();
        for (StructEntry field : fields) {
            int overriddenApiVersion = field.overrideApiVersion(apiVersion);
            CodecContext fieldCodecContext = (overriddenApiVersion != apiVersion)
                ? context.withApiVersion(overriddenApiVersion)
                : context;
            Object fieldValue = field.getter().apply(value);
            field.encode(fieldValue, buffer, fieldCodecContext);
        }
    }

    private Struct metadataFor(Class<?> type) {
        return metadata.computeIfAbsent(type, Struct::new);
    }
}
