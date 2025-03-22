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
import com.datastax.oss.driver.internal.core.util.Strings;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.LocalTime;

import static com.ing.data.cassandra.jdbc.codec.SqlTimeCodec.DEFAULT_TIME_FORMAT;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.NULL_KEYWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SqlTimeCodecTest {

    private final SqlTimeCodec sut = new SqlTimeCodec();
    private final Time NOW = Time.valueOf(LocalTime.now());
    private final long NOW_AS_NANOSECONDS = NOW.toLocalTime().toNanoOfDay();

    @Test
    void givenCodec_whenGetJavaType_returnTimeType() {
        assertEquals(GenericType.of(Time.class), sut.getJavaType());
    }

    @Test
    void givenCodec_whenGetCqlType_returnTime() {
        assertEquals(DataTypes.TIME, sut.getCqlType());
    }

    @Test
    void givenNullValue_whenEncode_returnNull() {
        assertNull(sut.encode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenEncode_returnByteBuffer() {
        ByteBuffer bytes = sut.encode(NOW, ProtocolVersion.DEFAULT);
        assertNotNull(bytes);
        assertEquals(NOW_AS_NANOSECONDS, bytes.getLong());
    }

    @Test
    void givenNullValue_whenDecode_returnNull() {
        assertNull(sut.decode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenDecode_returnTime() {
        ByteBuffer bytes = ByteBuffer.allocate(8).putLong(NOW_AS_NANOSECONDS);
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
        assertEquals(NOW, sut.parse(Strings.quote(NOW.toString())));
    }

    @Test
    void givenNullValue_whenFormat_returnNull() {
        assertEquals(NULL_KEYWORD, sut.format(null));
    }

    @Test
    void givenNonNullValue_whenFormat_returnExpectedValue() {
        assertEquals(Strings.quote(DEFAULT_TIME_FORMAT.format(NOW.toLocalTime())), sut.format(NOW));
    }
}
