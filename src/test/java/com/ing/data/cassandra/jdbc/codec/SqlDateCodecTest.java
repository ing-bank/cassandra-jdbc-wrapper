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
import java.sql.Date;
import java.time.LocalDate;

import static com.ing.data.cassandra.jdbc.codec.SqlDateCodec.signedToUnsigned;
import static com.ing.data.cassandra.jdbc.codec.SqlDateCodec.unsignedToSigned;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.NULL_KEYWORD;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SqlDateCodecTest {

    private final SqlDateCodec sut = new SqlDateCodec();
    private final Date NOW = Date.valueOf(LocalDate.now());
    private final long NOW_AS_EPOCH_DAYS = NOW.toLocalDate().toEpochDay();

    @Test
    void givenCodec_whenGetJavaType_returnDateType() {
        assertEquals(GenericType.of(Date.class), sut.getJavaType());
    }

    @Test
    void givenCodec_whenGetCqlType_returnDate() {
        assertEquals(DataTypes.DATE, sut.getCqlType());
    }

    @Test
    void givenNullValue_whenEncode_returnNull() {
        assertNull(sut.encode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenEncode_returnByteBuffer() {
        ByteBuffer bytes = sut.encode(NOW, ProtocolVersion.DEFAULT);
        assertNotNull(bytes);
        assertEquals(unsignedToSigned((int) NOW_AS_EPOCH_DAYS), bytes.getInt());
    }

    @Test
    void givenNullValue_whenDecode_returnNull() {
        assertNull(sut.decode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenDecode_returnDate() {
        ByteBuffer bytes = ByteBuffer.allocate(4).putInt(signedToUnsigned((int) NOW_AS_EPOCH_DAYS));
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
        assertEquals(Strings.quote(ISO_LOCAL_DATE.format(NOW.toLocalDate())), sut.format(NOW));
    }
}
