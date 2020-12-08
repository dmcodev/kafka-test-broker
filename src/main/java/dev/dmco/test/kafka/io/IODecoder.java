package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IODecoder {

    private final Map<Integer, Class<?>> requestTypes;

    public IODecoder() {
        requestTypes = RequestHandler.loadAll().stream()
            .map(RequestHandler::getHandledRequestType)
            .collect(Collectors.toMap(type -> type.getAnnotation(Request.class).key(), Function.identity()));
    }

    public RequestMessage decode(ByteBuffer buffer) {
        int apiKey = buffer.getShort();
        int apiVersion = buffer.getShort();
        buffer.rewind();
        CodecContext codecContext = new CodecContext()
            .set(ContextProperty.VERSION, apiVersion);
        return (RequestMessage) ObjectCodec.decode(buffer, getRequestType(apiKey),codecContext);
    }

    private Class<?> getRequestType(int apiKey) {
        return Optional.ofNullable(requestTypes.get(apiKey))
            .orElseThrow(() -> new BrokerException("API key not supported: " + apiKey, ErrorCode.INVALID_REQUEST));
    }
}
