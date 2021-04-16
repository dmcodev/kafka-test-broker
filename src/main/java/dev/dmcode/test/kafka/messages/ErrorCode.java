package dev.dmcode.test.kafka.messages;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@Accessors(fluent = true)
public enum ErrorCode {

    UNKNOWN_SERVER_ERROR(-1),
    NO_ERROR(0),
    ILLEGAL_GENERATION(22),
    INCONSISTENT_GROUP_PROTOCOL(23),
    UNKNOWN_MEMBER_ID(25),
    REBALANCE_IN_PROGRESS(27),
    UNSUPPORTED_VERSION(35),
    INVALID_REQUEST(42),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(43),
    OFFSET_NOT_AVAILABLE(78);

    private static final Map<Short, ErrorCode> MAPPING = Arrays.stream(ErrorCode.values())
        .collect(Collectors.toMap(ErrorCode::value, Function.identity()));

    private final short value;

    ErrorCode(int value) {
        this.value = (short) value;
    }

    public static ErrorCode of(short value) {
        return Optional.ofNullable(MAPPING.get(value))
            .orElseThrow(() -> new IllegalArgumentException("No error code with value: " + value));
    }
}
