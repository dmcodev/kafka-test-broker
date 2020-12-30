package dev.dmco.test.kafka.io.buffer

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer

class ResponseBufferSpec : StringSpec({

    "Initial position should be zero" {
        ResponseBuffer().position() shouldBe 0
    }

    "Should read written values" {
        val buffer = ResponseBuffer()
        buffer.putShort(42).putLong(43523L).putInt(541).putInt(964)
        ByteBuffer.wrap(buffer.read(2, 8)).asLongBuffer().get() shouldBe 43523L
        repeat(2) {
            val ints = ByteBuffer.wrap(buffer.read(10, 8)).asIntBuffer()
            ints.get() shouldBe 541
            ints.get() shouldBe 964
        }
    }
})