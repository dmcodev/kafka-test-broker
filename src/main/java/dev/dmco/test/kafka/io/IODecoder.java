package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.messages.ApiKeys;
import dev.dmco.test.kafka.messages.ApiVersionsRequest;
import dev.dmco.test.kafka.messages.RequestHeader;
import dev.dmco.test.kafka.messages.RequestMessage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class IODecoder {

    public static RequestMessage decode(ByteBuffer buffer) {
        RequestHeader header = readHeader(buffer);
        switch (header.apiKey()) {
            case ApiKeys.API_VERSIONS_KEY:
                return decodeApiVersionsRequest(header);
            default:
                throw new BrokerException("API key not supported: " + header.apiKey(), ErrorCode.INVALID_REQUEST);
        }
    }

    private static ApiVersionsRequest decodeApiVersionsRequest(RequestHeader header) {
        return ApiVersionsRequest.builder()
            .header(header)
            .build();
    }

    private static RequestHeader readHeader(ByteBuffer buffer) {
        return RequestHeader.builder()
            .apiKey(buffer.getShort())
            .apiVersion(buffer.getShort())
            .correlationId(buffer.getInt())
            .build();
    }

    private static String readNullableString(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return null;
        }
        int length = buffer.getShort();
        if (length == -1) {
            return null;
        }
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new String(chars, StandardCharsets.UTF_8);
    }
}
