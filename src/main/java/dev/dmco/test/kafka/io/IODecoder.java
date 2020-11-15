package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.handlers.RequestHandler;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.struct.StructCodec;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.request.RequestMessage;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IODecoder {

    private final StructCodec codec = new StructCodec();
    private final Map<Integer, Class<?>> requestTypes;

    public IODecoder() {
        requestTypes = RequestHandler.loadAll().stream()
            .flatMap(handler -> handler.handledRequestTypes().stream())
            .collect(Collectors.toMap(type -> type.getAnnotation(Request.class).apiKey(), Function.identity()));
    }

    public RequestMessage decode(ByteBuffer buffer) {
        int apiKey = buffer.getShort();
        int apiVersion = buffer.getShort();
        buffer.rewind();
        Class<?> requestType = getRequestType(apiKey);
        CodecContext codecContext = CodecContext.builder()
            .messageType(requestType)
            .apiVersion(apiVersion)
            .structCodec(codec)
            .build();
        return (RequestMessage) codec.decode(requestType, buffer, codecContext);
    }

    private Class<?> getRequestType(int apiKey) {
        return Optional.ofNullable(requestTypes.get(apiKey))
            .orElseThrow(() -> new BrokerException("API key not supported: " + apiKey, ErrorCode.INVALID_REQUEST));
    }
}
