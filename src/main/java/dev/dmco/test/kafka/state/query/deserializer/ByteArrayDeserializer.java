package dev.dmco.test.kafka.state.query.deserializer;

import java.util.Arrays;

public class ByteArrayDeserializer implements RecordDeserializer<byte[], byte[], byte[]> {

    @Override
    public byte[] deserializeKey(byte[] key) {
        return Arrays.copyOf(key, key.length);
    }

    @Override
    public byte[] deserializeValue(byte[] value) {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public byte[] deserializeHeaderValue(byte[] headerValue) {
        return Arrays.copyOf(headerValue, headerValue.length);
    }
}
