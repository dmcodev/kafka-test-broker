package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.bytes.BytesCodec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.Records;
import dev.dmco.test.kafka.messages.Records.Record;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

// TODO: current records format
public class RecordsCodec implements Codec {

    private static final int RECORD_VERSION_OFFSET = 16;
    private static final int MESSAGE_ATTRIBUTES_OFFSET = RECORD_VERSION_OFFSET + 1;
    private static final int MESSAGE_TIMESTAMP_OFFSET = MESSAGE_ATTRIBUTES_OFFSET + 1;

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
        byte recordsVersion = getRecordsVersion(buffer);
        if (recordsVersion == 0 || recordsVersion == 1) {
            Compression compression = getMessageCompression(buffer);
            if (compression == Compression.NONE) {
                return decodeMessageSet(buffer, recordsVersion, length);
            }
            return decodeCompressedMessageSet(buffer, recordsVersion, compression);
        } else {
            throw versionNotSupportedException(recordsVersion);
        }
    }

    private Records decodeMessageSet(ByteBuffer buffer, byte recordsVersion, int length) {
        int endPosition = buffer.position() + length;
        buffer.limit(buffer.position() + length);
        List<Record> records = new ArrayList<>();
        while (buffer.position() < endPosition) {
            int messageStartPosition = buffer.position();
            long offset = buffer.getLong();
            buffer.position(messageStartPosition + MESSAGE_TIMESTAMP_OFFSET);
            if (recordsVersion == 1) {
                buffer.getLong();
            }
            Record record = Record.builder()
                .offset(offset)
                .key((byte[]) BytesCodec.decode(buffer))
                .value((byte[]) BytesCodec.decode(buffer))
                .build();
            records.add(record);
        }
        return Records.builder()
            .version(recordsVersion)
            .records(records)
            .build();
    }

    private Records decodeCompressedMessageSet(ByteBuffer buffer, byte recordsVersion, Compression compression) {
        buffer.position(buffer.position() + MESSAGE_TIMESTAMP_OFFSET);
        if (recordsVersion == 1) {
            buffer.getLong();
        }
        buffer.getInt();
        byte[] compressedMessages = (byte[]) BytesCodec.decode(buffer);
        ByteBuffer decompressedMessages = ByteBuffer.wrap(compression.decompress(compressedMessages));
        return decodeMessageSet(decompressedMessages, recordsVersion, decompressedMessages.remaining());
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
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Records records = (Records) value;
        byte recordsVersion = (byte) records.version();
        if (recordsVersion == 0 || recordsVersion == 1) {
            encodeMessageSet(records.records(), recordsVersion, buffer);
        } else {
            throw versionNotSupportedException(recordsVersion);
        }
    }

    private void encodeMessageSet(List<Record> records, byte recordsVersion, ResponseBuffer buffer) {
        ByteBuffer messageSetSizeSlot = buffer.putSlot(Integer.BYTES);
        int sizeStartPosition = buffer.position();
        for (Record record : records) {
            encodeMessage(record, recordsVersion, buffer);
        }
        int messageSetSize = buffer.position() - sizeStartPosition;
        messageSetSizeSlot.putInt(messageSetSize);
    }

    private void encodeMessage(Record record, byte recordsVersion, ResponseBuffer buffer) {
        buffer.putLong(record.offset());
        ByteBuffer sizeSlot = buffer.putSlot(Integer.BYTES);
        int sizeStartPosition = buffer.position();
        ByteBuffer checkSumSlot = buffer.putSlot(Integer.BYTES);
        int checksumInputStartPosition = buffer.position();
        buffer.putByte(recordsVersion);
        buffer.putByte((byte) 0);
        if (recordsVersion == 1) {
            buffer.putLong(System.currentTimeMillis());
        }
        BytesCodec.encode(record.key(), buffer);
        BytesCodec.encode(record.value(), buffer);
        int messageEndPosition = buffer.position();
        byte[] checksumInput = buffer.read(checksumInputStartPosition, messageEndPosition - checksumInputStartPosition);
        int checksum = computeMessageChecksum(checksumInput);
        checkSumSlot.putInt(checksum);
        int size = messageEndPosition - sizeStartPosition;
        sizeSlot.putInt(size);
    }

    private int computeMessageChecksum(byte[] message) {
        CRC32 checksum = new CRC32();
        checksum.update(message);
        return (int) checksum.getValue();
    }

    private RuntimeException versionNotSupportedException(int recordsVersion) {
        return new IllegalArgumentException("Records version (magic) " + recordsVersion + " not supported");
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
                    throw new IllegalArgumentException("Unsupported compression type: " + Integer.toBinaryString(attributes));
            }
        }
    }
}
