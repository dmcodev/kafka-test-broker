package dev.dmco.test.kafka.io.codec.records;

import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;

enum Compression {

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

    static Compression from(int attributes) {
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
