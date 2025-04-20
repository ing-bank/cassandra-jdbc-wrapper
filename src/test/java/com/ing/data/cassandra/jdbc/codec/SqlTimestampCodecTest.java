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
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import com.datastax.oss.driver.internal.core.util.Strings;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static com.ing.data.cassandra.jdbc.codec.SqlTimestampCodec.DEFAULT_TIMESTAMP_FORMAT;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.NULL_KEYWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SqlTimestampCodecTest {

    private final SqlTimestampCodec sut = new SqlTimestampCodec();
    private final Instant NOW_INSTANT = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private final Timestamp NOW_SQL_TS = Timestamp.from(NOW_INSTANT);
    private final long NOW_AS_MILLISECONDS = NOW_INSTANT.toEpochMilli();

    @Test
    void givenCodec_whenGetJavaType_returnTimestampType() {
        assertEquals(GenericType.of(Timestamp.class), sut.getJavaType());
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
        ByteBuffer bytes = sut.encode(NOW_SQL_TS, ProtocolVersion.DEFAULT);
        assertNotNull(bytes);
        assertEquals(NOW_AS_MILLISECONDS, bytes.getLong());
    }

    @Test
    void givenNullValue_whenDecode_returnNull() {
        assertNull(sut.decode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenDecode_returnTimestamp() {
        ByteBuffer bytes = ByteBuffer.allocate(8).putLong(NOW_AS_MILLISECONDS);
        bytes.position(0);
        assertEquals(NOW_SQL_TS, sut.decode(bytes, ProtocolVersion.DEFAULT));
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
        final String formattedTimestamp = DEFAULT_TIMESTAMP_FORMAT.format(NOW_INSTANT.atZone(ZoneId.systemDefault()));
        assertEquals(NOW_SQL_TS, sut.parse(Strings.quote(formattedTimestamp)));
    }

    @Test
    void givenNullValue_whenFormat_returnNull() {
        assertEquals(NULL_KEYWORD, sut.format(null));
    }

    @Test
    void givenNonNullValue_whenFormat_returnExpectedValue() {
        assertEquals(Strings.quote(DEFAULT_TIMESTAMP_FORMAT.format(NOW_INSTANT)), sut.format(NOW_SQL_TS));
    }
}
