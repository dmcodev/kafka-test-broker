package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;

import java.nio.ByteBuffer;

public class ResponseEncoder {

    public ByteBuffer encode(ResponseMessage response, RequestHeader requestHeader) {
        int version = requestHeader.apiVersion();
        ResponseBuffer buffer = new ResponseBuffer();
        ResponseHeader responseHeader = ResponseHeader.builder()
            .correlationId(requestHeader.correlationId())
            .build();
        ResponseMessage responseWithHeader = response.withHeader(responseHeader);
        CodecContext codecContext = CodecContext.builder().version(version).build();
        ObjectCodec.encode(responseWithHeader, buffer, codecContext);
        return buffer.toByteBuffer();
    }
}
