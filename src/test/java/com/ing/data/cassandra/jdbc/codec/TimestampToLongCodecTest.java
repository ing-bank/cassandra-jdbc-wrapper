/*
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
package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;

import static com.ing.data.cassandra.jdbc.utils.Utils.NULL_KEYWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TimestampToLongCodecTest {

    private final TimestampToLongCodec sut = new TimestampToLongCodec();
    private final long NOW = Instant.now().toEpochMilli();

    @Test
    void givenCodec_whenGetJavaType_returnLong() {
        assertEquals(GenericType.LONG, sut.getJavaType());
    }

    @Test
    void givenCodec_whenGetCqlType_returnTimestamp() {
        assertEquals(DataTypes.TIMESTAMP, sut.getCqlType());
    }

    @Test
    void givenNullValue_whenEncode_returnNull() {
        assertNull(sut.encode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenEncode_returnByteBuffer() {
        ByteBuffer bytes = sut.encode(NOW, ProtocolVersion.DEFAULT);
        assertNotNull(bytes);
        assertEquals(NOW, bytes.getLong());
    }

    @Test
    void givenNullValue_whenDecode_returnNull() {
        assertNull(sut.decode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenDecode_returnLong() {
        ByteBuffer bytes = ByteBuffer.allocate(8).putLong(NOW);
        bytes.position(0);
        assertEquals(NOW, sut.decode(bytes, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenNullOrEmptyValue_whenParse_returnNull() {
        assertNull(sut.parse(null));
        assertNull(sut.parse(NULL_KEYWORD));
        assertNull(sut.parse(StringUtils.EMPTY));
        assertNull(sut.parse(StringUtils.SPACE));
    }

    @Test
    void givenNonNullValue_whenParse_returnExpectedValue() {
        assertEquals(NOW, sut.parse(String.valueOf(NOW)));
    }

    @Test
    void givenNullValue_whenFormat_returnNull() {
        assertEquals(NULL_KEYWORD, sut.format(null));
    }

    @Test
    void givenNonNullValue_whenFormat_returnExpectedValue() {
        assertEquals(String.valueOf(NOW), sut.format(NOW));
    }
}
