package dev.dmco.test.kafka.io.struct;

import dev.dmco.test.kafka.messages.common.Records;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

public enum FieldType {

    BOOLEAN {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(Boolean.class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            return buffer.get() != 0;
        }

        @Override
        public int encodedSize(Object value) {
            return 1;
        }

        @Override
        public int encodedNullSize() {
            return 1;
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            Boolean bool = (Boolean) value;
            buffer.put((byte) (bool ? 1 : 0));
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            buffer.put((byte) 0);
        }
    },

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

    STRING {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(String.class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            int length = buffer.getShort();
            if (length < 0) {
                throw new IllegalStateException(STRING.name() + " length must not be a negative number");
            }
            byte[] chars = new byte[length];
            buffer.get(chars);
            return new String(chars, StandardCharsets.UTF_8);
        }

        @Override
        public int encodedSize(Object value) {
            String string = (String) value;
            int length = string.getBytes(StandardCharsets.UTF_8).length;
            return INT16_SIZE + length;
        }

        @Override
        public int encodedNullSize() {
            throw new IllegalStateException(STRING.name() + " value must not be null");
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
            throw new IllegalStateException(STRING.name() + " value must not be null");
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
            return STRING.encodedSize(value);
        }

        @Override
        public int encodedNullSize() {
            return INT16_SIZE;
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            STRING.encode(value, buffer);
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
    },

    NULLABLE_BYTES {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(byte[].class);
        }

        @Override
        public byte[] decode(ByteBuffer buffer) {
            int length = buffer.getInt();
            if (length != -1) {
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                return bytes;
            }
            return null;
        }

        @Override
        public int encodedSize(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodedNullSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }
    },

    RECORDS {
        @Override
        public Set<Class<?>> compatibleJavaTypes() {
            return Collections.singleton(Records.class);
        }

        @Override
        public Object decode(ByteBuffer buffer) {
            byte[] bytes = (byte[]) NULLABLE_BYTES.decode(buffer);
            return new Records(new ArrayList<>());
        }

        @Override
        public int encodedSize(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int encodedNullSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void encode(Object value, ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void encodeNull(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
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
