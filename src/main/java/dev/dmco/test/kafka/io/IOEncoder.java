package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.messages.request.RequestHeader;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class IOEncoder {

    private final Map<Class<?>, TypeMetadata> typeMetadata = new HashMap<>();

    public ResponseBuffer encode(ResponseMessage response, RequestHeader header) {
        return ResponseBuffer.builder()
            .header(encodeHeader(header))
            .body(encodeBody(response, header.apiVersion()))
            .build();
    }

    public int calculateSize(Object instance, int apiVersion) {
        return getMetadata(instance.getClass())
            .fieldsForApiVersion(apiVersion).stream()
            .mapToInt(field -> field.calculateSize(instance, apiVersion, this))
            .sum();
    }

    public void encode(Object instance, int apiVersion, ByteBuffer buffer) {
        getMetadata(instance.getClass())
            .fieldsForApiVersion(apiVersion)
            .forEach(field -> field.encode(instance, apiVersion, buffer, this));
    }

    private ByteBuffer encodeBody(ResponseMessage response, short apiVersion) {
        int size = calculateSize(response, apiVersion);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        encode(response, apiVersion, buffer);
        return buffer;
    }

    private TypeMetadata getMetadata(Class<?> type) {
        return typeMetadata.computeIfAbsent(type, TypeMetadata::new);
    }

    private static ByteBuffer encodeHeader(RequestHeader header) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(header.correlationId());
        return buffer;
    }
}
