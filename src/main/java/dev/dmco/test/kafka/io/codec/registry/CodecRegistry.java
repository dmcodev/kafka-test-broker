package dev.dmco.test.kafka.io.codec.registry;

import dev.dmco.test.kafka.io.codec.Codec;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CodecRegistry {

    private static final Map<Type, Codec> CODEC_MAPPING = new HashMap<>();
    private static final Map<Type, Codec> CODEC_BY_TYPE_KEY = new HashMap<>();

    static {
        ServiceLoader.load(Codec.class)
            .forEach(codec -> codec.handledTypes().forEach(type -> CODEC_MAPPING.put(type, codec)));
    }

    public static Codec getCodec(Type key) {
        return CODEC_BY_TYPE_KEY.computeIfAbsent(key, CodecRegistry::select);
    }

    private static Codec select(Type key) {
        return CODEC_MAPPING.keySet().stream()
            .collect(Collectors.toMap(Function.identity(), candidateKey -> candidateKey.differenceFactor(key)))
            .entrySet().stream()
            .filter(entry -> entry.getValue() != Integer.MAX_VALUE)
            .min(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .map(CODEC_MAPPING::get)
            .orElseThrow(() -> new IllegalStateException("No suitable codec found for key: " + key));
    }
}
