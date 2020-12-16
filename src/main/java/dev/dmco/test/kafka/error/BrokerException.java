package dev.dmco.test.kafka.error;

import dev.dmco.test.kafka.messages.ErrorCode;

public class BrokerException extends RuntimeException {

    public BrokerException(String message, ErrorCode errorCode) {
        super("[" + errorCode.name() + "] " + message);
    }
}
