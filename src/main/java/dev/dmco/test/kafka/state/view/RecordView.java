package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.messages.Record;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.Accessors;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Value
@Builder
@Accessors(fluent = true)
public class RecordView {

    @FunctionalInterface
    public interface BytesDecoder<T> {
        T decode(byte[] bytes) throws Exception;
    }

    int partitionIndex;
    long offset;
    byte[] key;
    byte[] value;
    Map<String, byte[]> headers;

    public byte[] key() {
        return copy(key);
    }

    public <T> T key(BytesDecoder<T> decoder) {
        return decode(key(), decoder);
    }

    public String keyString() {
        return decodeString(key, StandardCharsets.UTF_8);
    }

    public String keyString(Charset charset) {
        return decodeString(key, charset);
    }

    public byte[] value() {
        return copy(value);
    }

    public <T> T value(BytesDecoder<T> decoder) {
        return decode(value(), decoder);
    }

    public String valueString() {
        return decodeString(value, StandardCharsets.UTF_8);
    }

    public String valueString(Charset charset) {
        return decodeString(value, charset);
    }

    public Map<String, byte[]> headers() {
        return headers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> copy(entry.getValue())));
    }

    public <T> Map<String, T> headers(BytesDecoder<T> decoder) {
        return headers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> decode(copy(entry.getValue()), decoder)));
    }

    public boolean containsHeader(String name) {
        return headers.containsKey(name);
    }

    public byte[] header(String name) {
        return copy(headers.get(name));
    }

    public <T> T header(String name, BytesDecoder<T> decoder) {
        return decode(header(name), decoder);
    }

    public String headerString(String name) {
        return decodeString(headers.get(name), StandardCharsets.UTF_8);
    }

    public String headerString(String name, Charset charset) {
        return decodeString(headers.get(name), charset);
    }

    private static byte[] copy(byte[] bytes) {
        return Optional.ofNullable(bytes).map(it -> Arrays.copyOf(it, it.length)).orElse(null);
    }

    @SneakyThrows
    private static <T> T decode(byte[] bytes, BytesDecoder<T> decoder) {
        return Objects.nonNull(bytes) ? decoder.decode(bytes) : null;
    }

    private static String decodeString(byte[] bytes, Charset charset) {
        return Optional.ofNullable(bytes).map(it -> new String(it, charset)).orElse(null);
    }

    static RecordView from(Record record, int partitionIndex) {
        return RecordView.builder()
            .partitionIndex(partitionIndex)
            .key(record.key().map(RecordView::copy).orElse(null))
            .value(record.value().map(RecordView::copy).orElse(null))
            .headers(
                record.headers().stream()
                    .filter(header -> header.value().isPresent())
                    .collect(Collectors.toMap(Record.Header::key, header -> header.value().map(RecordView::copy).get()))
            )
            .build();
    }
}
