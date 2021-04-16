package dev.dmcode.test.kafka.logging;

public interface Logger {

    void debug(String message);

    void debug(String format, Object... arguments);

    void debug(String message, Throwable throwable);

    static Logger create(Class<?> type) {
        return create(type.getName());
    }

    static Logger create(String name) {
        if (Slf4jLogger.SLF4J_AVAILABLE) {
            return new Slf4jLogger(name);
        }
        return new NoOpLogger();
    }
}
