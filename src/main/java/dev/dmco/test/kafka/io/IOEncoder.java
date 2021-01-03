package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;

import java.nio.ByteBuffer;

public class IOEncoder {

    public ByteBuffer encode(ResponseMessage response, RequestHeader requestHeader) {
        int apiVersion = requestHeader.apiVersion();
        ResponseBuffer buffer = new ResponseBuffer();
        ResponseHeader responseHeader = ResponseHeader.builder()
            .correlationId(requestHeader.correlationId())
            .build();
        ResponseMessage responseWithHeader = response.withHeader(responseHeader);
        CodecContext codecContext = new CodecContext()
            .set(ContextProperty.VERSION, apiVersion);
        ObjectCodec.encode(responseWithHeader, buffer, codecContext);
        return buffer.toByteBuffer();
    }
}
