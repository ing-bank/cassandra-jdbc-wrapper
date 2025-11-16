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

package com.ing.data.cassandra.jdbc.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serializer for {@link ByteBuffer}s in the context of a JSON returned by a CQL query.
 * <p>
 *     This serializer will convert the {@link ByteBuffer} content into a hexadecimal representation of the byte array
 *     to be managed as CQL type {@code blob} on Cassandra side.
 * </p>
 */
public class CassandraBlobSerializer extends JsonSerializer<ByteBuffer> {

    private static final char[] HEX_CHARS_ARRAY = "0123456789ABCDEF".toCharArray();

    @Override
    public void serialize(final ByteBuffer value,
                          final JsonGenerator gen,
                          final SerializerProvider serializers) throws IOException {
        if (value != null) {
            gen.writeString(byteArrayToHexString(value.array()));
        } else {
            gen.writeNull();
        }
    }

    private static String byteArrayToHexString(final byte[] bytes) {
        final char[] hexChars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            final int v = bytes[i] & 0xFF;
            hexChars[i * 2] = HEX_CHARS_ARRAY[v >>> 4];
            hexChars[i * 2 + 1] = HEX_CHARS_ARRAY[v & 0x0F];
        }
        return String.format("0x%s", new String(hexChars));
    }

}
