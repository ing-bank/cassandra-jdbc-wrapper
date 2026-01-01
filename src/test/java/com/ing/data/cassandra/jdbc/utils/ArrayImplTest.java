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

package com.ing.data.cassandra.jdbc.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.ASCII;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.CUSTOM;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.TIMEUUID;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.VARCHAR;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_NULL_TYPE_FOR_ARRAY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArrayImplTest {

    private static final Array EMPTY_ARRAY = new ArrayImpl();
    private static final List<String> SAMPLE_ITEMS = List.of("item1", "item2", "item3", "item4", "item5");

    @Test
    void givenNoData_whenConstructDefaultArray_returnEmptyArray() throws SQLException {
        assertArrayEquals(new Object[0], (Object[]) EMPTY_ARRAY.getArray());
    }

    @Test
    void givenEmptyArray_whenGetBaseTypeName_returnCustomTypeName() throws SQLException {
        assertEquals(CUSTOM.asLowercaseCql(), EMPTY_ARRAY.getBaseTypeName());
    }

    @Test
    void givenEmptyArray_whenGetBaseType_returnOtherType() throws SQLException {
        assertEquals(Types.OTHER, EMPTY_ARRAY.getBaseType());
    }

    @Test
    void givenListOfObjects_whenConstructArray_returnArray() throws SQLException {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertArrayEquals(SAMPLE_ITEMS.toArray(), sut.getArray());
    }

    @Test
    void givenArray_whenGetBaseTypeName_returnExpectedTypeName() {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertEquals(ASCII.asLowercaseCql(), sut.getBaseTypeName());
    }

    @Test
    void givenArray_whenGetBaseType_returnExpectedJdbcType() {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertEquals(Types.VARCHAR, sut.getBaseType());
    }

    @Test
    void givenListOfObjectsWithNullType_whenConstructArray_throwException() {
        final SQLException sqlEx = assertThrows(SQLException.class, () -> new ArrayImpl(SAMPLE_ITEMS, null));
        assertEquals(INVALID_NULL_TYPE_FOR_ARRAY, sqlEx.getMessage());
    }

    @Test
    void givenListOfObjectsWithUnsupportedCqlType_whenConstructArray_throwException() {
        final SQLException sqlEx = assertThrows(SQLException.class, () -> new ArrayImpl(SAMPLE_ITEMS, "my_type"));
        assertEquals("The specified CQL type is not supported: my_type", sqlEx.getMessage());
    }

    @Test
    void givenListOfUnconvertibleObjectsWithCqlType_whenConstructArray_throwException() {
        final SQLException sqlEx = assertThrows(SQLException.class,
            () -> new ArrayImpl(SAMPLE_ITEMS, TIMEUUID.asLowercaseCql()));
        assertThat(sqlEx.getMessage(), startsWith(
            "Conversion of an array item of type java.lang.String to class java.util.UUID(timeuuid) failed:"
        ));
    }

    static Stream<Arguments> buildTypedArrayTestCases() {
        final List<String> dataIncludingNullValues = new ArrayList<>();
        dataIncludingNullValues.add("test_item");
        dataIncludingNullValues.add(null);
        dataIncludingNullValues.add("test_item2");

        return Stream.of(
            Arguments.of(dataIncludingNullValues, VARCHAR.cqlType, new String[]{"test_item", null, "test_item2"}),
            Arguments.of(List.of("a", 'b'), ASCII.cqlType, new String[]{"a", "b"})
            /* TODO: implement test for all supported types and conversions
            Arguments.of(ASCII.cqlType, new String[]{ "a", "1", "c", "true" }, new Object[]{ 'a', 1, "c", true }),
            Arguments.of(TEXT.cqlType, new String[]{ "d", "2", "f", "false" }, new Object[]{ 'd', 2, "f", false }),
            Arguments.of(VARCHAR.cqlType, new String[]{ "g", "3", "i", "true" }, new Object[]{ 'g', 3, "i", true }),
            Arguments.of(BIGINT.cqlType, new Long[]{ 10L, 20L, 30L, 40L },
                new Object[]{ 10, 20L, "30", Long.valueOf("40")}),
            Arguments.of(COUNTER.cqlType, new Long[]{ 50L, 60L, 70L, 80L },
                new Object[]{ 50, 60L, "70", Long.valueOf("80") }),
            Arguments.of(BLOB.cqlType, new ByteBuffer[]{bytes(100), bytes("test")}, new Object[]{100, "test"}),
            Arguments.of(CUSTOM.cqlType, new ByteBuffer[]{bytes(50.9), bytes("c")}, new Object[]{50.9d, 'c'}),
            Arguments.of(DATE.cqlType, new Date[]{new Date(nowInMillis)},
            new Object[]{new java.util.Date(nowInMillis)}),
            Arguments.of(DECIMAL.cqlType, new BigDecimal[]{
                new BigDecimal("36.85"), new BigDecimal("37.9"), new BigDecimal("38.04"), new BigDecimal("39.211"),
                new BigDecimal("40.0")
            }, new Object[]{ 36.85, 37.9d, "38.04", new BigDecimal("39.211"), Double.valueOf("40") }),
            Arguments.of(DOUBLE.cqlType, new Double[]{ 15.5, 7.8, 3.6, 20.2 },
                new Object[]{ 15.5, 7.8f, "3.6", Double.valueOf("20.2") }),
            Arguments.of(DURATION.cqlType, new CqlDuration[]{
                CqlDuration.from("P21W"), CqlDuration.from("P1YT6H30M35S"), CqlDuration.from("PT360H")
            }, new Object[]{ "P21W", CqlDuration.from("P1YT6H30M35S"), Duration.ofDays(15) }),
            Arguments.of(FLOAT.cqlType, new Float[]{ 24.4f, 3.0f, 16.5f, 87.98f, 15.3f },
                new Object[]{ 24.4d, 3, 16.5f, "87.98", Float.valueOf("15.3") }),
            Arguments.of(INET.cqlType, new InetAddress[]{
                InetAddress.getByName("127.0.0.1"), InetAddress.getByName("::1")
            }, new Object[]{ "127.0.0.1", "::1" }),
            Arguments.of(INT.cqlType, new Integer[]{ 1, 2, 3, 4 }, new Object[]{ 1, Integer.valueOf("2"), "3", '4' }),
            Arguments.of(SMALLINT.cqlType, new Short[]{ 1, 2, 3, 4 }, new Object[]{ 1, Short.valueOf("2"), "3", '4' }),
            Arguments.of(TIME.cqlType, new Object[]{  }, new Object[]{  }),
            Arguments.of(TIMESTAMP.cqlType, new Object[]{  }, new Object[]{  }),
            Arguments.of(TIMEUUID.cqlType, new UUID[]{ uuid, java.util.UUID.fromString(uuidAsString) },
                new Object[]{ uuid, uuidAsString }),
            Arguments.of(UUID.cqlType, new UUID[]{ uuid, java.util.UUID.fromString(uuidAsString) },
                new Object[]{ uuid, uuidAsString }),
            Arguments.of(TINYINT.cqlType, new Byte[]{ 1, 2, 3, 4 }, new Object[]{ 1, Byte.valueOf("2"), "3", '4' }),
            Arguments.of(VARINT.cqlType, new BigInteger[]{
                new BigInteger("36"), new BigInteger("37"), new BigInteger("38"), new BigInteger("39")
            }, new Object[]{ Integer.valueOf("36"), 37, "38", new BigInteger("39") })
        */
        );
    }

    @ParameterizedTest
    @MethodSource("buildTypedArrayTestCases")
    void givenListOfObjectsWithCqlType_whenConstructArray_returnExpectedArray(
        final List<?> data, final String cqlType, final Object[] expectedArray
    ) throws SQLException {
        final var sut = new ArrayImpl(data, cqlType);
        assertArrayEquals(expectedArray, sut.getArray());
    }

    @Test
    void givenArray_whenGetArraySlice_returnExpectedArray() throws SQLException {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertArrayEquals(new String[]{"item3", "item4"}, (Object[]) sut.getArray(3, 2));
    }

    @Test
    void givenArray_whenGetArrayWithConversionMap_throwSQLFeatureNotSupportedException() {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertThrows(SQLFeatureNotSupportedException.class, () -> sut.getArray(Map.of("text", ByteBuffer.class)));
    }

    @Test
    void givenArray_whenGetResultSetWithConversionMap_throwSQLFeatureNotSupportedException() {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertThrows(SQLFeatureNotSupportedException.class, () -> sut.getResultSet(Map.of("text", ByteBuffer.class)));
    }

    @Test
    void givenArray_whenFree_emptyArray() throws SQLException {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        sut.free();
        assertArrayEquals(new Object[0], sut.getArray());
    }

}
