package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.struct.StructHandle;
import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.messages.request.RequestHeader;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IOEncoder {

    private final Map<Class<?>, StructHandle> typeMetadata = new HashMap<>();

    public Collection<ByteBuffer> encode(ResponseMessage response, RequestHeader header) {
        int apiVersion = header.apiVersion();
        return Arrays.asList(encodeHeader(header), encodeBody(response, apiVersion));
    }

    public int encodedSize(Object instance, int apiVersion) {
        return getMetadata(instance.getClass())
            .fieldsForApiVersion(apiVersion).stream()
            .mapToInt(field -> field.encodedSize(instance, apiVersion, this))
            .sum();
    }

    public void encode(Object instance, int apiVersion, ByteBuffer buffer) {
        getMetadata(instance.getClass())
            .fieldsForApiVersion(apiVersion)
            .forEach(field -> field.encode(instance, apiVersion, buffer, this));
    }

    private ByteBuffer encodeBody(ResponseMessage response, int apiVersion) {
        int size = encodedSize(response, apiVersion);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        encode(response, apiVersion, buffer);
        return buffer;
    }

    private StructHandle getMetadata(Class<?> type) {
        return typeMetadata.computeIfAbsent(type, StructHandle::new);
    }

    private static ByteBuffer encodeHeader(RequestHeader header) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(header.correlationId());
        return buffer;
    }
}
