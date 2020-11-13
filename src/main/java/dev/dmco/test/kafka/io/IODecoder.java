package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.io.struct.FieldHandle;
import dev.dmco.test.kafka.io.struct.StructHandle;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.meta.Request;
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
    private final Map<Class<?>, StructHandle> typeMetadata = new HashMap<>();

    public IODecoder(BrokerState brokerState) {
        requestTypeByApiKey = brokerState.getSupportedRequestTypes()
            .stream()
            .collect(Collectors.toMap(type -> type.getAnnotation(Request.class).apiKey(), Function.identity()));
    }

    public RequestMessage decode(ByteBuffer buffer) {
        int apiKey = buffer.getShort();
        int apiVersion = buffer.getShort();
        buffer.rewind();
        Class<?> requestType = getRequestType(apiKey);
        return (RequestMessage) decode(buffer, apiVersion, requestType);
    }

    @SneakyThrows
    public Object decode(ByteBuffer buffer, int apiVersion, Class<?> targetType) {
        StructHandle metadata = getMetadata(targetType);
        Constructor<?> constructor = metadata.constructor();
        Collection<FieldHandle> fields = metadata.fieldsForApiVersion(apiVersion);
        List<Object> constructorArguments = new ArrayList<>();
        for (FieldHandle field : fields) {
            Object fieldValue = field.decode(buffer, apiVersion, this);
            constructorArguments.add(fieldValue);
        }
        for (int i = constructorArguments.size(); i < constructor.getParameterCount(); i++) {
            Class<?> parameterType = constructor.getParameterTypes()[i];
            constructorArguments.add(createNullValue(parameterType));
        }
        return constructor.newInstance(constructorArguments.toArray());
    }

    private Object createNullValue(Class<?> parameterType) {
        if (Object.class.isAssignableFrom(parameterType)) {
            return null;
        } else {
            if (byte.class == parameterType) {
                return (byte) 0;
            } else if (short.class == parameterType) {
                return (short) 0;
            } else if (int.class == parameterType) {
                return 0;
            } else if (float.class == parameterType) {
                return 0.0f;
            } else if (double.class == parameterType) {
                return 0.0;
            } else {
                throw new IllegalArgumentException("Unsupported type: " + parameterType.getName());
            }
        }
    }

    private StructHandle getMetadata(Class<?> type) {
        return typeMetadata.computeIfAbsent(type, StructHandle::new);
    }

    private Class<?> getRequestType(int apiKey) {
        return Optional.ofNullable(requestTypeByApiKey.get(apiKey))
            .orElseThrow(() -> new BrokerException("API key not supported: " + apiKey, ErrorCode.INVALID_REQUEST));
    }
}
