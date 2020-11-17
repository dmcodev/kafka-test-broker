package dev.dmco.test.kafka.messages.meta;

import dev.dmco.test.kafka.io.codec.value.ValueType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TypeOverride {

    ValueType value();

    int sinceApiVersion();
}
