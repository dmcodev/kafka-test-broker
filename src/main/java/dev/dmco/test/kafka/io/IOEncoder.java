package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.struct.FieldHandle;
import dev.dmco.test.kafka.io.struct.StructHandle;
import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.messages.request.RequestHeader;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class IOEncoder {

    private final Map<Class<?>, StructHandle> typeMetadata = new HashMap<>();

    public ByteBuffer encode(ResponseMessage response, RequestHeader header) {
        int apiVersion = header.apiVersion();
        int size = encodedSize(response, apiVersion);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        encode(response, apiVersion, buffer);
        return buffer;
    }

    public int encodedSize(Object instance, int apiVersion) {
        return getFieldsFor(instance, apiVersion)
            .mapToInt(field -> field.encodedSize(instance, field.effectiveVersion(apiVersion), this))
            .sum();
    }

    public void encode(Object instance, int apiVersion, ByteBuffer buffer) {
        getFieldsFor(instance, apiVersion)
            .forEach(field -> field.encode(instance, field.effectiveVersion(apiVersion), buffer, this));
    }

    private StructHandle getMetadata(Class<?> type) {
        return typeMetadata.computeIfAbsent(type, StructHandle::new);
    }

    private Stream<FieldHandle> getFieldsFor(Object instance, int apiVersion) {
        return getMetadata(instance.getClass())
            .fields().stream()
            .filter(field -> field.presentInVersion(apiVersion));
    }
}
