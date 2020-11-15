package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersions;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseHeader;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class HeaderCodec implements ValueTypeCodec {

    private final Map<Class<?>, MessageTypeMetadata> metadata = new HashMap<>();

    private final Class<?> headerType;

    protected HeaderCodec(Class<?> headerType) {
        this.headerType = headerType;
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return context.structCodec()
            .decode(headerType, buffer, withHeaderVersion(context));
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        context.structCodec()
            .encode(value, buffer, withHeaderVersion(context));
    }

    private CodecContext withHeaderVersion(CodecContext context) {
        int headerVersion = metadataFor(context.messageType())
            .getHeaderVersion(context.apiVersion());
        return context.withApiVersion(headerVersion);
    }

    private MessageTypeMetadata metadataFor(Class<?> type) {
        return metadata.computeIfAbsent(type, MessageTypeMetadata::new);
    }

    public static class RequestHeaderCodec extends HeaderCodec {

        public RequestHeaderCodec() {
            super(RequestHeader.class);
        }
    }

    public static class ResponseHeaderCodec extends HeaderCodec {

        public ResponseHeaderCodec() {
            super(ResponseHeader.class);
        }
    }

    private static class MessageTypeMetadata {

        private final Map<Integer, Integer> headerVersionsMapping = new HashMap<>();
        private final Class<?> messageType;
        private final List<HeaderVersion> headerVersions;

        MessageTypeMetadata(Class<?> type) {
            messageType = type;
            headerVersions = Optional.ofNullable(type.getAnnotation(HeaderVersion.class))
                .map(Collections::singletonList)
                .orElseGet(() ->
                    Optional.ofNullable(type.getAnnotation(HeaderVersions.class))
                        .map(HeaderVersions::value)
                        .map(mappings -> Arrays.asList(mappings))
                        .orElseThrow(() -> new IllegalStateException("Header versions not defined for: " + type))
                );
        }

        int getHeaderVersion(int apiVersion) {
            return headerVersionsMapping.computeIfAbsent(apiVersion, this::calculateHeaderVersion);
        }

        private int calculateHeaderVersion(int apiVersion) {
            return headerVersions.stream()
                .sorted(Comparator.comparingInt(HeaderVersion::sinceApiVersion).reversed())
                .filter(mapping -> apiVersion >= mapping.sinceApiVersion())
                .map(HeaderVersion::value)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No suitable header version for api version " + apiVersion + " and type " + messageType));
        }
    }
}
