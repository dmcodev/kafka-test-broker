package dev.dmcode.test.kafka.state.query.deserializer;

import java.util.Arrays;

public class ByteArrayDeserializer implements RecordDeserializer<byte[]> {

    @Override
    public byte[] deserialize(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length);
    }
}
