package dev.dmco.test.kafka.io.codec.registry

import dev.dmco.test.kafka.io.codec.bytes.BytesCodec
import dev.dmco.test.kafka.io.codec.generic.CollectionCodec
import dev.dmco.test.kafka.io.codec.primitives.BooleanCodec
import dev.dmco.test.kafka.io.codec.primitives.Int16Codec
import dev.dmco.test.kafka.io.codec.primitives.Int32Codec
import dev.dmco.test.kafka.io.codec.primitives.Int64Codec
import dev.dmco.test.kafka.io.codec.primitives.Int8Codec
import dev.dmco.test.kafka.io.codec.registry.Type.of
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
            row(of(Boolean::class.java), BooleanCodec::class.java),
            row(of(Byte::class.java), Int8Codec::class.java),
            row(of(Short::class.java), Int16Codec::class.java),
            row(of(Int::class.java), Int32Codec::class.java),
            row(of(Long::class.java), Int64Codec::class.java),
            row(of(ByteArray::class.java), BytesCodec::class.java),
            row(of(String::class.java), StringCodec::class.java),
            row(of(Optional::class.java, of(String::class.java)), NullableStringCodec::class.java),
            row(of(Tag::class.java), TagsCodec::class.java),
            row(of(List::class.java, of(String::class.java)), CollectionCodec::class.java),
            row(of(List::class.java, of(Any::class.java)), CollectionCodec::class.java),
            row(of(Set::class.java, of(String::class.java)), CollectionCodec::class.java),
            row(of(Set::class.java, of(java.lang.Integer::class.java)), CollectionCodec::class.java),
            row(of(Collection::class.java, of(java.lang.Boolean::class.java)), CollectionCodec::class.java),
            row(of(Collection::class.java, of(ProduceRequest.Record::class.java)), RecordsCodec::class.java),
        ) { typeKey, expectedCodecType ->

            CodecRegistry.getCodec(typeKey)::class.java shouldBeSameInstanceAs expectedCodecType
        }
    }
})