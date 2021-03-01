package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.messages.Record;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Value;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Value
@Builder
@Getter(AccessLevel.NONE)
public class RecordView {

    @FunctionalInterface
    public interface BytesDecoder<T> {
        T decode(byte[] bytes) throws Exception;
    }

    int partitionIndex;
    byte[] key;
    byte[] value;
    Map<String, byte[]> headers;

    public int partitionIndex() {
        return partitionIndex;
    }

    public byte[] key() {
        return copy(key);
    }

    public String keyString() {
        return keyString(StandardCharsets.UTF_8);
    }

    public String keyString(Charset charset) {
        return new String(key(), charset);
    }

    public <T> T key(BytesDecoder<T> decoder) {
        return decode(key(), decoder);
    }

    public byte[] value() {
        return copy(value);
    }

    public String valueString() {
        return valueString(StandardCharsets.UTF_8);
    }

    public String valueString(Charset charset) {
        return new String(value(), charset);
    }

    public <T> T value(BytesDecoder<T> decoder) {
        return decode(value(), decoder);
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
        return Optional.ofNullable(headers.get(name))
            .map(RecordView::copy)
            .orElseThrow(() -> new IllegalArgumentException("Header " + name + " does not exist"));
    }

    public String headerString(String name) {
        return headerString(headerString(name), StandardCharsets.UTF_8);
    }

    public String headerString(String name, Charset charset) {
        return new String(header(name), charset);
    }

    public <T> T header(String name, BytesDecoder<T> decoder) {
        return decode(header(name), decoder);
    }

    private static byte[] copy(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length);
    }

    @SneakyThrows
    private static <T> T decode(byte[] bytes, BytesDecoder<T> decoder) {
        return decoder.decode(bytes);
    }

    public static RecordView from(Record record, int partitionIndex) {
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
