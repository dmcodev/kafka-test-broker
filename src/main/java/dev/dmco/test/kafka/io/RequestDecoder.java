package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.state.RequestHandlers;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RequestDecoder {

    private final Map<Integer, Class<?>> keyToRequestTypeMapping;

    public RequestDecoder(RequestHandlers requestHandlers) {
        keyToRequestTypeMapping = requestHandlers.handledRequestTypes().stream()
            .collect(Collectors.toMap(type -> type.getAnnotation(Request.class).key(), Function.identity()));
    }

    public RequestMessage decode(ByteBuffer buffer) {
        int key = buffer.getShort();
        int version = buffer.getShort();
        buffer.rewind();
        CodecContext codecContext = CodecContext.builder().version(version).build();
        return (RequestMessage) ObjectCodec.decode(buffer, getRequestType(key),codecContext);
    }

    private Class<?> getRequestType(int key) {
        return Optional.ofNullable(keyToRequestTypeMapping.get(key))
            .orElseThrow(() -> new IllegalStateException("Request key not supported: " + key));
    }
}
