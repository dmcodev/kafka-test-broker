package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;

public enum ValueType implements ValueTypeCodec {

    INT8(Int8Codec.class),
    INT16(Int16Codec.class),
    INT32(Int32Codec.class),
    INT64(Int64Codec.class),
    VARINT(VarIntCodec.class),
    UVARINT(VarUIntCodec.class),
    BOOLEAN(BooleanCodec.class),
    STRING(StringCodec.class),
    NULLABLE_STRING(NullableStringCodec.class),
    RECORDS(RecordsCodec.class),
    BYTES(BytesCodec.class),
    TAGS_BUFFER(TagsCodec.class),
    REQUEST_HEADER(HeaderCodec.RequestHeaderCodec.class),
    RESPONSE_HEADER(HeaderCodec.ResponseHeaderCodec.class);

    private final ValueTypeCodec codec;

    @SneakyThrows
    ValueType(Class<? extends ValueTypeCodec> codecType) {
        codec = codecType.newInstance();
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return codec.decode(buffer, context);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        codec.encode(value, buffer, context);
    }
}
