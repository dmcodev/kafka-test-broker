package dev.dmco.test.kafka.logging;

public interface Logger {

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
