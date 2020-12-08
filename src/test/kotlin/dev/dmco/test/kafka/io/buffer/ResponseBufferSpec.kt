package dev.dmco.test.kafka.io.buffer

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer

class ResponseBufferSpec : StringSpec({

    "Initial position should be zero" {
        ResponseBuffer().position() shouldBe 0
    }

    "Should read written values" {
        val buffer = ResponseBuffer(3)
        buffer.putShort(42).putInt(541).putInt(964)
        repeat(2) {
            val ints = ByteBuffer.wrap(buffer.read(2, 8)).asIntBuffer()
            ints.get() shouldBe 541
            ints.get() shouldBe 964
        }
    }
})