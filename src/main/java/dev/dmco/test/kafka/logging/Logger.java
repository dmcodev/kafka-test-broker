package dev.dmco.test.kafka.logging;

public interface Logger {

    void info(String format, Object... arguments);

    void warn(String message);

    void warn(String message, Throwable throwable);

    void error(String message, Throwable throwable);

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
