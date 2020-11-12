package dev.dmco.test.kafka.error;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public enum ErrorCode {

    UNKNOWN_SERVER_ERROR(-1),
    UNSUPPORTED_VERSION(35),
    INVALID_REQUEST(42),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(43);

    private final int value;
}
