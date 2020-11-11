package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.error.BrokerException;
import dev.dmco.test.kafka.error.ErrorCode;
import dev.dmco.test.kafka.messages.ApiKeys;
import dev.dmco.test.kafka.messages.ApiVersionsResponse;
import dev.dmco.test.kafka.messages.RequestHeader;
import dev.dmco.test.kafka.messages.ResponseMessage;

import java.nio.ByteBuffer;

public class IOEncoder {

    public static ResponseBuffer encode(ResponseMessage response, RequestHeader header) {
        ResponseBuffer.ResponseBufferBuilder builder = ResponseBuffer.builder()
            .header(encodeHeader(header));
        ByteBuffer body;
        switch (header.apiKey()) {
            case ApiKeys.API_VERSIONS_KEY:
                body = encodeApiVersionsResponse((ApiVersionsResponse) response);
                break;
            default:
                throw new BrokerException("API key not supported: " + header.apiKey(), ErrorCode.INVALID_REQUEST);
        }
        return builder.body(body)
            .build();
    }

    private static ByteBuffer encodeApiVersionsResponse(ApiVersionsResponse response) {
        ByteBuffer buffer = ByteBuffer.allocate(6 + 6 * response.apiKeys().size());
        buffer.putShort((short) response.errorCode());
        buffer.putInt(response.apiKeys().size());
        response.apiKeys().forEach(apiKey -> {
            buffer.putShort((short) apiKey.apiKey());
            buffer.putShort((short) apiKey.minVersion());
            buffer.putShort((short) apiKey.maxVersion());
        });
        return buffer;
    }

    private static ByteBuffer encodeHeader(RequestHeader header) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(header.correlationId());
        return buffer;
    }
}
