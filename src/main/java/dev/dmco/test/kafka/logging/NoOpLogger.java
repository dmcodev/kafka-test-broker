package dev.dmco.test.kafka.logging;

public class NoOpLogger implements Logger {

    @Override
    public void debug(String message) {}

    @Override
    public void debug(String format, Object... arguments) {}

    @Override
    public void debug(String message, Throwable throwable) {}
}
