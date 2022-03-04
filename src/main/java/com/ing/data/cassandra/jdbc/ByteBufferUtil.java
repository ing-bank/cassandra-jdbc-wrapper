package com.ing.data.cassandra.jdbc;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

public class ByteBufferUtil {
    public static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }


    public static ByteBuffer bytes(byte b) {
        return ByteBuffer.allocate(1).put(0, b);
    }

    public static ByteBuffer bytes(short s) {
        return ByteBuffer.allocate(2).putShort(0, s);
    }

    public static ByteBuffer bytes(int i) {
        return ByteBuffer.allocate(4).putInt(0, i);
    }

    public static ByteBuffer bytes(long n) {
        return ByteBuffer.allocate(8).putLong(0, n);
    }

    public static ByteBuffer bytes(float f) {
        return ByteBuffer.allocate(4).putFloat(0, f);
    }

    public static ByteBuffer bytes(double d) {
        return ByteBuffer.allocate(8).putDouble(0, d);
    }

    public static int toInt(ByteBuffer bytes) {
        return bytes.getInt(bytes.position());
    }

    public static short toShort(ByteBuffer bytes) {
        return bytes.getShort(bytes.position());
    }

    public static long toLong(ByteBuffer bytes) {
        return bytes.getLong(bytes.position());
    }

    public static float toFloat(ByteBuffer bytes) {
        return bytes.getFloat(bytes.position());
    }

    public static double toDouble(ByteBuffer bytes) {
        return bytes.getDouble(bytes.position());
    }

    public static byte[] getArray(ByteBuffer buffer) {
        int length = buffer.remaining();
        if (buffer.hasArray()) {
            int boff = buffer.arrayOffset() + buffer.position();
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        } else {
            byte[] bytes = new byte[length];
            buffer.duplicate().get(bytes);
            return bytes;
        }
    }

}
