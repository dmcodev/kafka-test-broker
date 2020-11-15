package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.struct.StructCodec;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;

import java.nio.ByteBuffer;
import java.util.List;

public class IOEncoder {

    private final StructCodec codec = new StructCodec();

    public List<ByteBuffer> encode(ResponseMessage response, RequestHeader header) {
        Class<?> responseType = response.getClass();
        int apiVersion = header.apiVersion();
        CodecContext codecContext = CodecContext.builder()
            .messageType(responseType)
            .apiVersion(apiVersion)
            .structCodec(codec)
            .build();
        ResponseBuffer buffer = new ResponseBuffer();
        codec.encode(response, buffer, codecContext);
        return buffer.buffers();
    }
}
