/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.ing.data.cassandra.jdbc.utils;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;

/**
 * Utility methods for {@link ByteBuffer} manipulation.
 * <p>
 *     This class is a partial copy of {@code org.apache.cassandra.utils.ByteBufferUtil} class from
 *     {@code cassandra-all} library.
 * </p>
 */
public final class ByteBufferUtil {

    private ByteBufferUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Encodes a {@code String} in a {@code ByteBuffer} using UTF-8.
     *
     * @param s The string to encode.
     * @return The encoded string.
     */
    public static ByteBuffer bytes(final String s) {
        return wrap(s.getBytes(UTF_8));
    }

    /**
     * Encodes a {@code byte} in a {@code ByteBuffer}.
     *
     * @param b The byte to encode.
     * @return The encoded byte.
     */
    public static ByteBuffer bytes(final byte b) {
        return allocate(1).put(0, b);
    }

    /**
     * Encodes a {@code short} in a {@code ByteBuffer}.
     *
     * @param s The short to encode.
     * @return The encoded short.
     */
    public static ByteBuffer bytes(final short s) {
        return allocate(2).putShort(0, s);
    }

    /**
     * Encodes an {@code int} in a {@code ByteBuffer}.
     *
     * @param i The int to encode.
     * @return The encoded int.
     */
    public static ByteBuffer bytes(final int i) {
        return allocate(4).putInt(0, i);
    }

    /**
     * Encodes a {@code long} in a {@code ByteBuffer}.
     *
     * @param n The long to encode.
     * @return The encoded long.
     */
    public static ByteBuffer bytes(final long n) {
        return allocate(8).putLong(0, n);
    }

    /**
     * Encodes a {@code float} in a {@code ByteBuffer}.
     *
     * @param f The float to encode.
     * @return The encoded float.
     */
    public static ByteBuffer bytes(final float f) {
        return allocate(4).putFloat(0, f);
    }

    /**
     * Encodes a {@code double} in a {@code ByteBuffer}.
     *
     * @param d The double to encode.
     * @return The encoded double.
     */
    public static ByteBuffer bytes(final double d) {
        return allocate(8).putDouble(0, d);
    }

    /**
     * Converts a {@code ByteBuffer} to an integer.
     * Does not change the byte buffer position.
     *
     * @param bytes The byte buffer to convert to integer.
     * @return The integer representation of the byte buffer.
     */
    public static int toInt(final ByteBuffer bytes) {
        return bytes.getInt(bytes.position());
    }

    /**
     * Converts a {@code ByteBuffer} to a short.
     * Does not change the byte buffer position.
     *
     * @param bytes The byte buffer to convert to short.
     * @return The short representation of the byte buffer.
     */
    public static short toShort(final ByteBuffer bytes) {
        return bytes.getShort(bytes.position());
    }

    /**
     * Converts a {@code ByteBuffer} to a long.
     * Does not change the byte buffer position.
     *
     * @param bytes The byte buffer to convert to long.
     * @return The long representation of the byte buffer.
     */
    public static long toLong(final ByteBuffer bytes) {
        return bytes.getLong(bytes.position());
    }

    /**
     * Converts a {@code ByteBuffer} to a float.
     * Does not change the byte buffer position.
     *
     * @param bytes The byte buffer to convert to float.
     * @return The float representation of the byte buffer.
     */
    public static float toFloat(final ByteBuffer bytes) {
        return bytes.getFloat(bytes.position());
    }

    /**
     * Converts a {@code ByteBuffer} to a double.
     * Does not change the byte buffer position.
     *
     * @param bytes The byte buffer to convert to double.
     * @return The double representation of the byte buffer.
     */
    public static double toDouble(final ByteBuffer bytes) {
        return bytes.getDouble(bytes.position());
    }

    /**
     * Gets a {@code byte} array representation of a {@code ByteBuffer}.
     *
     * @param buffer The byte buffer to represent as an array.
     * @return The array representation of the byte buffer.
     */
    public static byte[] getArray(final ByteBuffer buffer) {
        final int length = buffer.remaining();
        if (buffer.hasArray()) {
            final int boff = buffer.arrayOffset() + buffer.position();
            return copyOfRange(buffer.array(), boff, boff + length);
        } else {
            final byte[] bytes = new byte[length];
            buffer.duplicate().get(bytes);
            return bytes;
        }
    }

}
