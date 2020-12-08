package dev.dmco.test.kafka.io.codec.records;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.Records;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

// TODO: current records format
public class RecordsCodec implements Codec {

    private static final int RECORD_VERSION_OFFSET = 16;

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Records.class));
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
        } else {
            throw versionNotSupportedException(version);
        }
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Records records = (Records) value;
        byte recordsVersion = (byte) records.version();
        if (recordsVersion == 0 || recordsVersion == 1) {
            LegacyRecordsEncoder.encode(records, buffer);
        } else {
            throw versionNotSupportedException(recordsVersion);
        }
    }

    private RuntimeException versionNotSupportedException(int recordsVersion) {
        return new IllegalArgumentException("Records version (magic) " + recordsVersion + " not supported");
    }
}
