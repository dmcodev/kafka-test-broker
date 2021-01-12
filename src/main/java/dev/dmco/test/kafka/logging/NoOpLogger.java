package dev.dmco.test.kafka.logging;

public class NoOpLogger implements Logger {

    @Override
    public void info(String format, Object... arguments) {}

    @Override
    public void warn(String message) {}

    @Override
    public void warn(String message, Throwable throwable) {}

    @Override
    public void error(String message, Throwable throwable) {}
}
