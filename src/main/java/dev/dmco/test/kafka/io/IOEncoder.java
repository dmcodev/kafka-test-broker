package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.registry.CodecRegistry;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;

import java.nio.ByteBuffer;
import java.util.List;

public class IOEncoder {

    public List<ByteBuffer> encode(ResponseMessage response, RequestHeader requestHeader) {
        int apiVersion = requestHeader.apiVersion();
        ResponseBuffer buffer = new ResponseBuffer();
        ResponseHeader responseHeader = ResponseHeader.builder()
            .correlationId(requestHeader.correlationId())
            .build();
        ResponseMessage responseWithHeader = response.withHeader(responseHeader);
        CodecContext codecContext = new CodecContext()
            .set(ContextProperty.VERSION, apiVersion);
        TypeKey responseTypeKey = TypeKey.key(response.getClass());
        CodecRegistry.getCodec(responseTypeKey)
            .encode(responseWithHeader, buffer, codecContext);
        return buffer.collect();
    }
}
