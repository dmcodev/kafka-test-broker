package dev.dmco.test.kafka.io.codec.registry;

import dev.dmco.test.kafka.io.codec.Codec;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CodecRegistry {

    private static final Map<TypeKey, Codec> CODEC_MAPPING = new HashMap<>();
    private static final Map<TypeKey, Codec> CODEC_BY_TYPE_KEY = new HashMap<>();
    private static final Map<Class<?>, Codec> CODEC_BY_CODEC_TYPE = new HashMap<>();

    static {
        ServiceLoader.load(Codec.class)
            .forEach(codec -> codec.handledTypes()
                .forEach(type -> CODEC_MAPPING.put(type, codec))
            );
    }

    public static Codec getCodec(TypeKey key) {
        return CODEC_BY_TYPE_KEY.computeIfAbsent(key, CodecRegistry::select);
    }

    public static <T extends Codec> T getCodec(Class<T> codecType) {
        return (T) CODEC_BY_CODEC_TYPE.computeIfAbsent(codecType, CodecRegistry::select);
    }

    private static Codec select(TypeKey key) {
        return CODEC_MAPPING.keySet().stream()
            .collect(Collectors.toMap(Function.identity(), candidateKey -> candidateKey.differenceFactor(key)))
            .entrySet().stream()
            .filter(entry -> entry.getValue() != Integer.MAX_VALUE)
            .min(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .map(CODEC_MAPPING::get)
            .orElseThrow(() -> new IllegalStateException("No suitable codec found for key: " + key));
    }

    private static Codec select(Class<?> codecType) {
        return CODEC_MAPPING.values().stream()
            .filter(codec -> codec.getClass().isAssignableFrom(codecType))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Could not find codec of type: " + codecType));
    }
}
