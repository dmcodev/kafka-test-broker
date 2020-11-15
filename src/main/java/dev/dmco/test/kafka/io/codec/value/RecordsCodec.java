package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.usecase.produce.ProduceRequest.Record;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class RecordsCodec implements ValueTypeCodec {

    private static final int RECORD_VERSION_OFFSET = 16;
    private static final int MESSAGE_ATTRIBUTES_OFFSET = RECORD_VERSION_OFFSET + 1;
    private static final int MESSAGE_TIMESTAMP_OFFSET = MESSAGE_ATTRIBUTES_OFFSET + 1;

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int length = buffer.getInt();
        if (length == -1) {
            return null;
        }
        byte recordsVersion = getRecordsVersion(buffer);
        if (recordsVersion == 0 || recordsVersion == 1) {
            Compression compression = getMessageCompression(buffer);
            if (compression == Compression.NONE) {
                return decodeMessageSet(buffer, recordsVersion, length, context);
            }
            return decodeCompressedMessageSet(buffer, recordsVersion, compression, context);
        } else {
            throw new IllegalArgumentException("Records version (magic) " + recordsVersion + " not supported");
        }
    }

    private List<Record> decodeMessageSet(ByteBuffer buffer, byte recordsVersion, int length, CodecContext context) {
        int endPosition = buffer.position() + length;
        buffer.limit(buffer.position() + length);
        List<Record> records = new ArrayList<>();
        while (buffer.position() < endPosition) {
            buffer.position(buffer.position() + MESSAGE_TIMESTAMP_OFFSET);
            if (recordsVersion == 1) {
                buffer.getLong();
            }
            Record record = Record.builder()
                .key((byte[]) ValueType.BYTES.decode(buffer, context))
                .value((byte[]) ValueType.BYTES.decode(buffer, context))
                .build();
            records.add(record);
        }
        return records;
    }

    private List<Record> decodeCompressedMessageSet(ByteBuffer buffer, byte recordsVersion, Compression compression, CodecContext context) {
        buffer.position(buffer.position() + MESSAGE_TIMESTAMP_OFFSET);
        if (recordsVersion == 1) {
            buffer.getLong();
        }
        buffer.getInt();
        byte[] compressedMessages = (byte[]) ValueType.BYTES.decode(buffer, context);
        ByteBuffer decompressedMessages = ByteBuffer.wrap(compression.decompress(compressedMessages));
        return decodeMessageSet(decompressedMessages, recordsVersion, decompressedMessages.remaining(), context);
    }

    private byte getRecordsVersion(ByteBuffer buffer) {
        buffer.mark();
        buffer.position(buffer.position() + RECORD_VERSION_OFFSET);
        byte version = buffer.get();
        buffer.reset();
        return version;
    }

    private Compression getMessageCompression(ByteBuffer buffer) {
        buffer.mark();
        buffer.position(buffer.position() + MESSAGE_ATTRIBUTES_OFFSET);
        byte attributes = buffer.get();
        buffer.reset();
        return Compression.from(attributes);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        throw new UnsupportedOperationException();
    }

    private enum Compression {

        NONE {
            @Override
            byte[] decompress(byte[] bytes) {
                return bytes;
            }
        },
        GZIP {
            @Override
            @SneakyThrows
            byte[] decompress(byte[] bytes) {
                ByteArrayOutputStream output = new ByteArrayOutputStream(bytes.length * 2);
                try (GZIPInputStream input = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
                    byte[] buffer = new byte[128];
                    int readLength;
                    while ((readLength = input.read(buffer)) > 0) {
                        output.write(buffer, 0, readLength);
                    }
                }
                return output.toByteArray();
            }
        },
        //SNAPPY,
        //LZ4,
        //ZSTD
        ;

        abstract byte[] decompress(byte[] bytes);

        static Compression from(byte attributes) {
            switch (attributes & 0b111) {
                case 0:
                    return NONE;
                case 1:
                    return GZIP;
//                case 2:
//                    return SNAPPY;
//                case 3:
//                    return LZ4;
//                case 4:
//                    return ZSTD;
                default:
                    throw new IllegalArgumentException("Unsupported compression type");
            }
        }
    }
}
