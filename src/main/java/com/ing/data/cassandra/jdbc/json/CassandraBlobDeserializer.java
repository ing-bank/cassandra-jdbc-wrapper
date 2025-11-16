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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Deserializer for {@link ByteBuffer}s in the context of a JSON returned by a CQL query.
 * <p>
 *     This deserializer expects an even-length string starting with {@code 0x} representing a byte array (CQL type
 *     {@code blob}).
 * </p>
 */
public class CassandraBlobDeserializer extends JsonDeserializer<ByteBuffer> {

    @Override
    public ByteBuffer deserialize(final JsonParser jsonParser,
                                  final DeserializationContext deserializationContext) throws IOException {
        final String value = jsonParser.getValueAsString();
        if (value != null) {
            return wrap(hexStringToByteArray(value));
        } else {
            return null;
        }
    }

    private static byte[] hexStringToByteArray(final String input) {
        final String hexString = input.replace("0x", EMPTY);
        final int len = hexString.length();
        if (!hexString.matches("[0-9A-Fa-f]*") || len % 2 != 0) {
            throw new IllegalArgumentException("Invalid value: expecting an even-length hexadecimal string.");
        }
        final byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }

}
