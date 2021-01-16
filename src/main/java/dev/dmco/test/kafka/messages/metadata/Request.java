package dev.dmco.test.kafka.messages.metadata;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Request {

    int key();

    int minVersion() default 0;

    int maxVersion() default 0;
}
