package dev.dmco.test.kafka.logging;

public class NoOpLogger implements Logger {

    @Override
    public void info(String format, Object... arguments) {}
}
