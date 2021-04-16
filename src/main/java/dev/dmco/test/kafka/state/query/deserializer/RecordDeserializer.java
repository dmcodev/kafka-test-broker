package dev.dmco.test.kafka.state.query.deserializer;

import java.nio.charset.Charset;

public interface RecordDeserializer<K, V, HV> {

    K deserializeKey(byte[] key);

    V deserializeValue(byte[] value);

    HV deserializeHeaderValue(byte[] headerValue);

    static RecordDeserializer<byte[], byte[], byte[]> bytes() {
        return new ByteArrayDeserializer();
    }

    static RecordDeserializer<String, String, String> string() {
        return new StringDeserializer();
    }

    static RecordDeserializer<String, String, String> string(Charset charset) {
        return new StringDeserializer(charset);
    }
}
