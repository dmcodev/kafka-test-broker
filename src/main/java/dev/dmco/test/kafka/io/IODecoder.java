package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.handlers.RequestHandler;
import dev.dmco.test.kafka.messages.KafkaRequest;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.state.BrokerState;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IODecoder {

    @FunctionalInterface
    private interface TypeDecoder<T> {
        T decode(ByteBuffer buffer);
    }

    private static final Map<Class<?>, TypeDecoder<?>> DEFAULT_DECODERS = new HashMap<>();

    static {
        DEFAULT_DECODERS.put(short.class, ByteBuffer::getShort);
        DEFAULT_DECODERS.put(int.class, ByteBuffer::getInt);
    }

    private final Map<Class<?>, TypeDecoder<?>> decoders = new HashMap<>();
    private final Map<Integer, Class<?>> apiKeyToRequestType;

    public IODecoder(BrokerState brokerState) {
        decoders.putAll(DEFAULT_DECODERS);
        apiKeyToRequestType = brokerState.handlersRegistry().getHandlers().stream()
            .map(RequestHandler::handledRequestTypes)
            .flatMap(Collection::stream)
            .filter(type -> type.isAnnotationPresent(KafkaRequest.class))
            .collect(Collectors.toMap(type -> type.getAnnotation(KafkaRequest.class).apiKey(), Function.identity()));
    }

    public RequestMessage decode(ByteBuffer buffer) {
        int apikey = buffer.getShort();
        buffer.rewind();
        Class<?> requestType = Optional.ofNullable(apiKeyToRequestType.get(apikey))
            .orElseThrow(() -> new BrokerException("API key not supported: " + apikey, ErrorCode.INVALID_REQUEST));
        return decode(buffer, requestType);
    }

    private RequestMessage decode(ByteBuffer buffer, Class<?> type) {
        return (RequestMessage) getDecoderFor(type).decode(buffer);
    }

    private TypeDecoder<?> getDecoderFor(Class<?> type) {
        if (decoders.containsKey(type)) {
            return decoders.get(type);
        }
        Constructor<?> constructor = Arrays.stream(type.getConstructors())
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("No suitable constructor found in: " + type));
        List<Function<ByteBuffer, Object>> fieldDecodingFunctions = new ArrayList<>(constructor.getParameterCount());
        for (Field field : type.getDeclaredFields()) {
            TypeDecoder<?> fieldDecoder = getDecoderFor(field.getType());
            fieldDecodingFunctions.add(fieldDecoder::decode);
        }
        TypeDecoder<?> objectDecoder = buffer -> decodeObject(buffer, constructor, fieldDecodingFunctions);
        decoders.put(type, objectDecoder);
        return objectDecoder;
    }

    @SneakyThrows
    private static Object decodeObject(
        ByteBuffer buffer,
        Constructor<?> constructor,
        List<Function<ByteBuffer, Object>> fieldDecodingFunctions
    ) {
        Object[] arguments = fieldDecodingFunctions.stream()
            .map(it -> it.apply(buffer))
            .toArray();
        return constructor.newInstance(arguments);
    }
}
