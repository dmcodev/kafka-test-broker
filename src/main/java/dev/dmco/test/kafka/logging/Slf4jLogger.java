package dev.dmco.test.kafka.logging;

import org.slf4j.LoggerFactory;

class Slf4jLogger implements Logger {

    public static final boolean SLF4J_AVAILABLE;

    static {
        Class<?> slf4jLoggerClass = null;
        try {
            slf4jLoggerClass = Class.forName("org.slf4j.Logger");
        } catch (ClassNotFoundException ex) {}
        SLF4J_AVAILABLE = slf4jLoggerClass != null;
    }

    private final org.slf4j.Logger logger;

    Slf4jLogger(String name) {
        logger = LoggerFactory.getLogger(name);
    }

    @Override
    public void info(String format, Object... arguments) {
        logger.info(format, arguments);
    }
}
