package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.struct.StructCodec;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;

import java.nio.ByteBuffer;
import java.util.List;

public class IOEncoder {

    private final StructCodec codec = new StructCodec();

    public List<ByteBuffer> encode(ResponseMessage response, RequestHeader requestHeader) {
        Class<?> responseType = response.getClass();
        int apiVersion = requestHeader.apiVersion();
        CodecContext codecContext = CodecContext.builder()
            .messageType(responseType)
            .apiVersion(apiVersion)
            .structCodec(codec)
            .build();
        ResponseBuffer buffer = new ResponseBuffer();
        ResponseHeader responseHeader = ResponseHeader.builder()
            .correlationId(requestHeader.correlationId())
            .build();
        ResponseMessage responseWithHeader = response.withHeader(responseHeader);
        codec.encode(responseWithHeader, buffer, codecContext);
        return buffer.collect();
    }
}
