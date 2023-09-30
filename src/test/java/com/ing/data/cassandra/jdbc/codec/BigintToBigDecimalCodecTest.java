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
import com.ing.data.cassandra.jdbc.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.NULL_KEYWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BigintToBigDecimalCodecTest {

    private final BigintToBigDecimalCodec sut = new BigintToBigDecimalCodec();

    @Test
    void givenCodec_whenGetJavaType_returnBigDecimal() {
        assertEquals(GenericType.BIG_DECIMAL, sut.getJavaType());
    }

    @Test
    void givenCodec_whenGetCqlType_returnBigInt() {
        assertEquals(DataTypes.BIGINT, sut.getCqlType());
    }

    @Test
    void givenNullValue_whenEncode_returnNull() {
        assertNull(sut.encode(null, ProtocolVersion.DEFAULT));
    }

    private static Stream<Arguments> buildEncodeTestCases() {
        return Stream.of(Arguments.of("6789", 6789), Arguments.of("12.345", 12), Arguments.of("12.9", 12));
    }

    @ParameterizedTest
    @MethodSource("buildEncodeTestCases")
    void givenValue_whenEncode_returnByteBuffer(final String encodedValue, final int expectedValue) {
        ByteBuffer bytes = sut.encode(new BigDecimal(encodedValue), ProtocolVersion.DEFAULT);
        assertNotNull(bytes);
        assertEquals(expectedValue, new BigInteger(ByteBufferUtil.getArray(bytes)).intValue());
    }

    @Test
    void givenNullValue_whenDecode_returnNull() {
        assertNull(sut.decode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    void givenValue_whenDecode_returnBigDecimal() {
        ByteBuffer bytes = ByteBuffer.allocate(8).putLong(12345);
        bytes.position(0);
        assertEquals(new BigDecimal(12345), sut.decode(bytes, ProtocolVersion.DEFAULT));
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
        assertEquals(new BigDecimal("12345"), sut.parse("12345"));
    }

    @Test
    void givenNullValue_whenFormat_returnNull() {
        assertEquals(NULL_KEYWORD, sut.format(null));
    }

    @Test
    void givenNonNullValue_whenFormat_returnExpectedValue() {
        assertEquals("12345", sut.format(new BigDecimal("12345")));
    }
}
