package dev.dmco.test.kafka.io.codec.records;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.Record;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

public class RecordsCodec implements Codec {

    private static final int RECORD_VERSION_OFFSET = 16;

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Collection.class, Type.of(Record.class)));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        int length = buffer.getInt();
        if (length == -1) {
            return null;
        }
        byte version = buffer.get(buffer.position() + RECORD_VERSION_OFFSET);
        if (version == 0 || version == 1) {
            return LegacyRecordsDecoder.decode(buffer, version, length);
        } if (version == 2) {
            return RecordsDecoder.decode(buffer, length);
        } else {
            throw versionNotSupportedException(version);
        }
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Collection<Record> records = (Collection<Record>) value;
        int version = context.version();
        if (version == 0 || version == 1) {
            LegacyRecordsEncoder.encode(records, buffer, version);
        } else if (version == 2) {
            LegacyRecordsEncoder.encode(records, buffer, 1);
            //RecordsEncoder.encode(records, buffer);
        } else {
            throw versionNotSupportedException(version);
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Collections.emptyList(), valueType, buffer, context);
    }

    private RuntimeException versionNotSupportedException(int version) {
        return new IllegalArgumentException("Records version (magic) " + version + " not supported");
    }
}
