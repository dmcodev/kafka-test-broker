package dev.dmcode.test.kafka.state.query.deserializer;

import lombok.RequiredArgsConstructor;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@RequiredArgsConstructor
public class StringDeserializer implements RecordDeserializer<String, String, String> {

    private final Charset charset;

    public StringDeserializer() {
        this(StandardCharsets.UTF_8);
    }

    @Override
    public String deserializeKey(byte[] key) {
        return deserialize(key);
    }

    @Override
    public String deserializeValue(byte[] value) {
        return deserialize(value);
    }

    @Override
    public String deserializeHeaderValue(byte[] headerValue) {
        return deserialize(headerValue);
    }

    private String deserialize(byte[] bytes) {
        return Objects.nonNull(bytes) ? new String(bytes, charset) : null;
    }
}
