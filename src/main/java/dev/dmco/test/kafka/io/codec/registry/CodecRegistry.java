package dev.dmco.test.kafka.io.codec.registry;

import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.bytes.BytesCodec;
import dev.dmco.test.kafka.io.codec.bytes.NullableBytesCodec;
import dev.dmco.test.kafka.io.codec.generic.CollectionCodec;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.io.codec.primitives.BooleanCodec;
import dev.dmco.test.kafka.io.codec.primitives.Int16Codec;
import dev.dmco.test.kafka.io.codec.primitives.Int32Codec;
import dev.dmco.test.kafka.io.codec.primitives.Int64Codec;
import dev.dmco.test.kafka.io.codec.primitives.Int8Codec;
import dev.dmco.test.kafka.io.codec.primitives.VarUIntCodec;
import dev.dmco.test.kafka.io.codec.strings.CompactStringCodec;
import dev.dmco.test.kafka.io.codec.strings.NullableStringCodec;
import dev.dmco.test.kafka.io.codec.strings.StringCodec;
import dev.dmco.test.kafka.io.codec.structs.RecordsCodec;
import dev.dmco.test.kafka.io.codec.structs.SubscriptionCodec;
import dev.dmco.test.kafka.io.codec.structs.TagsCodec;
import dev.dmco.test.kafka.messages.Subscription;
import dev.dmco.test.kafka.messages.Tag;
import dev.dmco.test.kafka.usecase.produce.ProduceRequest;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static dev.dmco.test.kafka.io.codec.registry.TypeKey.key;

public class CodecRegistry {

    public static final BooleanCodec BOOLEAN = new BooleanCodec();
    public static final Int8Codec INT_8 = new Int8Codec();
    public static final Int16Codec INT_16 = new Int16Codec();
    public static final Int32Codec INT_32 = new Int32Codec();
    public static final Int64Codec INT_64 = new Int64Codec();
    public static final VarUIntCodec VAR_UINT = new VarUIntCodec();
    public static final StringCodec STRING = new StringCodec();
    public static final CompactStringCodec COMPACT_STRING = new CompactStringCodec();
    public static final NullableStringCodec NULLABLE_STRING = new NullableStringCodec();
    public static final BytesCodec BYTES = new BytesCodec();

    private static final Map<TypeKey, Function<TypeKey, Codec>> CODEC_FACTORY_MAPPING = new HashMap<>();

    static {
        addFactory(key(boolean.class), (key) -> BOOLEAN);
        addFactory(key(Boolean.class), (key) -> BOOLEAN);
        addFactory(key(byte.class), (key) -> INT_8);
        addFactory(key(Byte.class), (key) -> INT_8);
        addFactory(key(short.class), (key) -> INT_16);
        addFactory(key(Short.class), (key) -> INT_16);
        addFactory(key(int.class), (key) -> INT_32);
        addFactory(key(Integer.class), (key) -> INT_32);
        addFactory(key(long.class), (key) -> INT_64);
        addFactory(key(Long.class), (key) -> INT_64);
        addFactory(key(byte[].class), (key) -> BYTES);
        addFactory(key(Optional.class, key(byte[].class)), (key) -> new NullableBytesCodec());
        addFactory(key(String.class), (key) -> STRING);
        addFactory(key(Optional.class, key(String.class)), (key) -> NULLABLE_STRING);
        addFactory(key(Collection.class, key(Object.class)), CollectionCodec::from);
        addFactory(key(Object.class), ObjectCodec::from);
        addFactory(key(Tag.class), (key) -> new TagsCodec());
        addFactory(key(Collection.class, key(ProduceRequest.Record.class)), (key) -> new RecordsCodec());
        addFactory(key(Subscription.class), (key) -> new SubscriptionCodec());
    }

    private static final Map<TypeKey, Codec> CODEC_MAPPING = new HashMap<>();

    public static Codec getCodec(TypeKey key) {
        return CODEC_MAPPING.computeIfAbsent(key, CodecRegistry::createCodec);
    }

    private static void addFactory(TypeKey key, Function<TypeKey, Codec> factoryFunction) {
        CODEC_FACTORY_MAPPING.put(key, factoryFunction);
    }

    private static Codec createCodec(TypeKey key) {
        return CODEC_FACTORY_MAPPING.keySet().stream()
            .collect(Collectors.toMap(Function.identity(), candidateKey -> candidateKey.differenceFactor(key)))
            .entrySet().stream()
            .filter(entry -> entry.getValue() != Integer.MAX_VALUE)
            .min(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .map(CODEC_FACTORY_MAPPING::get)
            .map(factory -> factory.apply(key))
            .orElseThrow(() -> new IllegalStateException("No codec factory found for key: " + key));
    }
}
