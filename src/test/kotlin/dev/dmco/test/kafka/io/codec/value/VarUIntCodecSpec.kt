package dev.dmco.test.kafka.io.codec.value

import dev.dmco.test.kafka.io.buffer.ResponseBuffer
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.row
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer

class VarUIntCodecSpec : StringSpec({

    val codec = VarUIntCodec()

    "Should decode unsigned VarInt" {
        forAll(
            row((0b10101100_00000010).shl(16), 300),
            row((0b10010110_00000001).shl(16), 150)
        ) { binary, decimal ->

            val buffer = ByteBuffer.allocate(4)
            buffer.putInt(binary)
            buffer.rewind()

            (codec.decode(buffer, null) as Int) shouldBe decimal

            val responseBuffer = ResponseBuffer()
            codec.encode(decimal, responseBuffer, null)

            val encodeBuffer = responseBuffer.collect().first()
            encodeBuffer.rewind()
            encodeBuffer.limit(encodeBuffer.capacity())

            encodeBuffer.int shouldBe binary
        }
    }
})