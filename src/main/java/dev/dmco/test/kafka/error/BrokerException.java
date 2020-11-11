package dev.dmco.test.kafka.error;

public class BrokerException extends RuntimeException {

    public BrokerException(String message, ErrorCode errorCode) {
        super("[" + errorCode.name() + "] " + message);
    }
}
