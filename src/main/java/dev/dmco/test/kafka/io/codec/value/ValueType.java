package dev.dmco.test.kafka.io.codec.value;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public enum ValueType {

    INT8(Int8Codec.class),
    INT16(Int16Codec.class),
    INT32(Int32Codec.class),
    INT64(Int64Codec.class),
    VARINT(VarintCodec.class),
    BOOLEAN(BooleanCodec.class),
    STRING(StringCodec.class),
    NULLABLE_STRING(NullableStringCodec.class),
    RECORDS(RecordsCodec.class),
    TAGS_BUFFER(TagsCodec.class);

    private final ValueTypeCodec codec;

    @SneakyThrows
    ValueType(Class<? extends ValueTypeCodec> codecType) {
        codec = codecType.newInstance();
    }
}
