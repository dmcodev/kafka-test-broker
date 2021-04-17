package dev.dmcode.test.kafka.state.query.deserializer;

import java.nio.charset.Charset;

@FunctionalInterface
public interface RecordDeserializer<V> {

    V deserialize(byte[] bytes);

    static RecordDeserializer<byte[]> bytes() {
        return new ByteArrayDeserializer();
    }

    static RecordDeserializer<String> string() {
        return new StringDeserializer();
    }

    static RecordDeserializer<String> string(Charset charset) {
        return new StringDeserializer(charset);
    }
}
