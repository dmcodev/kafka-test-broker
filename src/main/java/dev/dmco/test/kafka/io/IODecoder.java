package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.messages.KafkaRequest;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.state.BrokerState;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IODecoder {

    private final Map<Integer, Class<?>> requestTypeByApiKey;
    private final Map<Class<?>, TypeMetadata> typeMetadata = new HashMap<>();

    public IODecoder(BrokerState brokerState) {
        requestTypeByApiKey = brokerState.getSupportedRequestTypes()
            .stream()
            .collect(Collectors.toMap(type -> type.getAnnotation(KafkaRequest.class).apiKey(), Function.identity()));
    }

    public RequestMessage decode(ByteBuffer buffer) {
        int apiKey = buffer.getShort();
        int apiVersion = buffer.getShort();
        buffer.rewind();
        Class<?> requestType = getRequestType(apiKey);
        return (RequestMessage) decode(requestType, buffer, apiVersion);
    }

    @SneakyThrows
    public Object decode(Class<?> type, ByteBuffer buffer, int apiVersion) {
        TypeMetadata metadata = getMetadata(type);
        Constructor<?> constructor = metadata.constructor();
        Collection<TypeField> fields = metadata.fieldsForApiVersion(apiVersion);
        List<Object> constructorArguments = new ArrayList<>();
        for (TypeField field : fields) {
            Object fieldValue = field.decode(buffer, apiVersion, this);
            constructorArguments.add(fieldValue);
        }
        return constructor.newInstance(constructorArguments.toArray());
    }

    private TypeMetadata getMetadata(Class<?> type) {
        return typeMetadata.computeIfAbsent(type, TypeMetadata::new);
    }

    private Class<?> getRequestType(int apiKey) {
        return Optional.ofNullable(requestTypeByApiKey.get(apiKey))
            .orElseThrow(() -> new BrokerException("API key not supported: " + apiKey, ErrorCode.INVALID_REQUEST));
    }
}
