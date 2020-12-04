package dev.dmco.test.kafka.io.codec.registry

import dev.dmco.test.kafka.io.codec.bytes.BytesCodec
import dev.dmco.test.kafka.io.codec.generic.CollectionCodec
import dev.dmco.test.kafka.io.codec.primitives.BooleanCodec
import dev.dmco.test.kafka.io.codec.primitives.Int16Codec
import dev.dmco.test.kafka.io.codec.primitives.Int32Codec
import dev.dmco.test.kafka.io.codec.primitives.Int64Codec
import dev.dmco.test.kafka.io.codec.primitives.Int8Codec
import dev.dmco.test.kafka.io.codec.registry.TypeKey.key
import dev.dmco.test.kafka.io.codec.strings.NullableStringCodec
import dev.dmco.test.kafka.io.codec.strings.StringCodec
import dev.dmco.test.kafka.io.codec.structs.RecordsCodec
import dev.dmco.test.kafka.io.codec.structs.TagsCodec
import dev.dmco.test.kafka.messages.Tag
import dev.dmco.test.kafka.usecase.produce.ProduceRequest
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.row
import io.kotest.matchers.types.shouldBeSameInstanceAs
import java.util.Optional

class CodecRegistrySpec : StringSpec({

    "Should return suitable codec for type key" {
        forAll(
            row(key(Boolean::class.java), BooleanCodec::class.java),
            row(key(Byte::class.java), Int8Codec::class.java),
            row(key(Short::class.java), Int16Codec::class.java),
            row(key(Int::class.java), Int32Codec::class.java),
            row(key(Long::class.java), Int64Codec::class.java),
            row(key(ByteArray::class.java), BytesCodec::class.java),
            row(key(String::class.java), StringCodec::class.java),
            row(key(Optional::class.java, key(String::class.java)), NullableStringCodec::class.java),
            row(key(Tag::class.java), TagsCodec::class.java),
            row(key(List::class.java, key(String::class.java)), CollectionCodec::class.java),
            row(key(List::class.java, key(Any::class.java)), CollectionCodec::class.java),
            row(key(Set::class.java, key(String::class.java)), CollectionCodec::class.java),
            row(key(Set::class.java, key(java.lang.Integer::class.java)), CollectionCodec::class.java),
            row(key(Collection::class.java, key(java.lang.Boolean::class.java)), CollectionCodec::class.java),
            row(key(Collection::class.java, key(ProduceRequest.Record::class.java)), RecordsCodec::class.java),
        ) { typeKey, expectedCodecType ->

            CodecRegistry.getCodec(typeKey)::class.java shouldBeSameInstanceAs expectedCodecType
        }
    }
})