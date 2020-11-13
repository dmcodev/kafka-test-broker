package dev.dmco.test.kafka.io.struct;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

public enum FieldType {

    INT8 {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(Byte.class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            return buffer.get();
        }

        @Override
        public int encodedSize(Object value) {
            return INT8_SIZE;
        }

        @Override
        public int encodedNullSize() {
            return INT8_SIZE;
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            buffer.put((byte) value);
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            encode((byte) 0, buffer);
        }
    },

    INT16 {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(Short.class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            return buffer.getShort();
        }

        @Override
        public int encodedSize(Object value) {
            return INT16_SIZE;
        }

        @Override
        public int encodedNullSize() {
            return INT16_SIZE;
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            buffer.putShort((short) value);
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            encode((short) 0, buffer);
        }
    },

    INT32 {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(Integer.class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            return buffer.getInt();
        }

        @Override
        public int encodedSize(Object value) {
            return INT32_SIZE;
        }

        @Override
        public int encodedNullSize() {
            return INT32_SIZE;
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            buffer.putInt((int) value);
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            encode(0, buffer);
        }
    },

    NULLABLE_STRING {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(String.class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            int length = buffer.getShort();
            if (length >= 0) {
                byte[] chars = new byte[length];
                buffer.get(chars);
                return new String(chars, StandardCharsets.UTF_8);
            }
            return null;
        }

        @Override
        public int encodedSize(Object value) {
            String string = (String) value;
            int length = string.getBytes(StandardCharsets.UTF_8).length;
            return INT16_SIZE + length;
        }

        @Override
        public int encodedNullSize() {
            return INT16_SIZE;
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            String string = (String) value;
            byte[] chars = string.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) chars.length);
            buffer.put(chars);
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            buffer.putShort((short) -1);
        }
    },

    TAGS_BUFFER {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(byte[].class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }

        @Override
        public int encodedSize(Object value) {
            // TODO: proper tag buffers handling
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodedNullSize() {
            return INT8_SIZE;
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            // TODO: proper tag buffers handling
            throw new UnsupportedOperationException();
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            buffer.put((byte) 0);
        }
    };

    public static final int INT8_SIZE = 1;
    public static final int INT16_SIZE = 2;
    public static final int INT32_SIZE = 4;

    public boolean compatibleWith(Class<?> javaType) {
        return compatibleJavaTypes().contains(javaType);
    }

    public abstract Set<Class<?>> compatibleJavaTypes();

    public abstract Object decode(ByteBuffer buffer);

    public abstract int encodedSize(Object value);

    public abstract int encodedNullSize();

    public abstract void encode(Object value, ByteBuffer buffer);

    public abstract void encodeNull(ByteBuffer buffer);
}
