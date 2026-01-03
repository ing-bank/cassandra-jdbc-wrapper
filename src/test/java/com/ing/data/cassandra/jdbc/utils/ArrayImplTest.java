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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.ASCII;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.BIGINT;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.BLOB;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.BOOLEAN;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.COUNTER;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.CUSTOM;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.DATE;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.DECIMAL;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.DOUBLE;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.DURATION;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.FLOAT;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.INET;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.INT;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.SMALLINT;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.TEXT;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.TIME;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.TIMESTAMP;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.TIMEUUID;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.TINYINT;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.VARCHAR;
import static com.ing.data.cassandra.jdbc.types.DataTypeEnum.VARINT;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.bytes;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_NULL_TYPE_FOR_ARRAY;
import static java.time.ZoneId.systemDefault;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.UUID.randomUUID;
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
    void givenArray_whenGetBaseTypeName_returnExpectedTypeName() throws SQLException {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertEquals(ASCII.asLowercaseCql(), sut.getBaseTypeName());
    }

    @Test
    void givenArray_whenGetBaseType_returnExpectedJdbcType() throws SQLException {
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

    static Stream<Arguments> buildTypedArrayTestCases() throws UnknownHostException {
        // Testing array containing null values
        final List<String> dataIncludingNullValues = new ArrayList<>();
        dataIncludingNullValues.add("test_item");
        dataIncludingNullValues.add(null);
        dataIncludingNullValues.add("test_item2");
        // BLOB and CUSTOM testing
        final ByteBuffer sampleByteBuffer = ByteBuffer.wrap(new byte[]{1, 2, 3});
        // TIMEUUID and UUID testing
        final UUID uuid = randomUUID();
        final String uuidAsString = "3d5eda6b-89eb-414b-bbd2-6b0b04856576";
        // DATE, TIME and TIMESTAMP testing
        final Instant nowInstant = Instant.now().truncatedTo(MILLIS);
        final Instant yesterdayInstant = nowInstant.minus(1, DAYS);
        final Instant tomorrowInstant = nowInstant.plus(1, DAYS);
        final Instant nextWeekInstant = nowInstant.plus(7, DAYS);
        final Instant previousHourInstant = nowInstant.minus(1, HOURS);
        final Instant twoHoursAgoInstant = nowInstant.minus(2, HOURS);
        final Instant nextHourInstant = nowInstant.plus(1, HOURS);
        final LocalDate nowLocalDate = LocalDate.ofInstant(nowInstant, systemDefault());
        final LocalDate yesterdayLocalDate = LocalDate.ofInstant(yesterdayInstant, systemDefault());
        final LocalDate tomorrowLocalDate = LocalDate.ofInstant(tomorrowInstant, systemDefault());
        final LocalDate nextWeekLocalDate = LocalDate.ofInstant(nextWeekInstant, systemDefault());
        final LocalTime nowLocalTime = LocalTime.ofInstant(nowInstant, systemDefault());
        final LocalTime previousHourLocalTime = LocalTime.ofInstant(previousHourInstant, systemDefault());
        final LocalTime nextHourLocalTime = LocalTime.ofInstant(nextHourInstant, systemDefault());
        final LocalTime twoHoursAgoLocalTime = LocalTime.ofInstant(twoHoursAgoInstant, systemDefault());
        final LocalDateTime nowLocalDateTime = LocalDateTime.ofInstant(nowInstant, systemDefault());
        final LocalDateTime yesterdayLocalDateTime = LocalDateTime.ofInstant(yesterdayInstant, systemDefault());
        final LocalDateTime tomorrowLocalDateTime = LocalDateTime.ofInstant(tomorrowInstant, systemDefault());
        final LocalDateTime nextWeekLocalDateTime = LocalDateTime.ofInstant(nextWeekInstant, systemDefault());
        final String sampleDateAsString = "2026-01-31";
        final String sampleTimeAsString = "14:30:25";
        final String sampleDateTimeAsString = "2026-01-31 14:30:25.678";

        return Stream.of(
            Arguments.of(dataIncludingNullValues, VARCHAR.asLowercaseCql(),
                new String[]{ "test_item", null, "test_item2" }),
            Arguments.of(List.of("a", 'b', 1, true), ASCII.asLowercaseCql(), new String[]{ "a", "b", "1", "true" }),
            Arguments.of(List.of("c", 'd', 2, false), TEXT.asLowercaseCql(), new String[]{ "c", "d", "2", "false" }),
            Arguments.of(List.of("e", 'f', 3, 4.5), VARCHAR.asLowercaseCql(), new String[]{ "e", "f", "3", "4.5" }),
            Arguments.of(List.of(true, false, "true", "yes", 1), BOOLEAN.asLowercaseCql(),
                new Boolean[]{ true, false, true, false, false}),
            Arguments.of(List.of(10, 20L, "30", Long.valueOf("40")), BIGINT.asLowercaseCql(),
                new Long[]{ 10L, 20L, 30L, 40L }),
            Arguments.of(List.of(50, 60L, "70", Long.valueOf("80")), COUNTER.asLowercaseCql(),
                new Long[]{ 50L, 60L, 70L, 80L }),
            Arguments.of(List.of("test", sampleByteBuffer, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE,
                    Long.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE),
                BLOB.asLowercaseCql(),
                new ByteBuffer[]{ bytes("test"), sampleByteBuffer, bytes(Byte.MAX_VALUE),
                    bytes(Short.MAX_VALUE), bytes(Integer.MAX_VALUE), bytes(Long.MAX_VALUE), bytes(Float.MAX_VALUE),
                    bytes(Double.MAX_VALUE) }
            ),
            Arguments.of(List.of("test2", sampleByteBuffer), CUSTOM.asLowercaseCql(),
                new ByteBuffer[]{ bytes("test2"), sampleByteBuffer }
            ),
            Arguments.of(List.of(Date.valueOf(nowLocalDate), yesterdayLocalDate,
                    new java.util.Date(tomorrowInstant.toEpochMilli()), nextWeekInstant, sampleDateAsString),
                DATE.asLowercaseCql(),
                new Date[]{ Date.valueOf(nowLocalDate), Date.valueOf(yesterdayLocalDate),
                    Date.valueOf(tomorrowLocalDate), Date.valueOf(nextWeekLocalDate), Date.valueOf(sampleDateAsString) }
            ),
            Arguments.of(List.of(36.85, 37.9d, "38.04", new BigDecimal("39.211"), Double.valueOf("40")),
                DECIMAL.asLowercaseCql(),
                new BigDecimal[]{ new BigDecimal("36.85"), new BigDecimal("37.9"), new BigDecimal("38.04"),
                    new BigDecimal("39.211"), new BigDecimal("40.0") }
            ),
            Arguments.of(List.of(15.5, 7.8f, "3.6", Double.valueOf("20.2")), DOUBLE.asLowercaseCql(),
                new Double[]{ 15.5, 7.8, 3.6, 20.2 }
            ),
            Arguments.of(List.of("P21W", CqlDuration.from("P1YT6H30M35S"), Duration.ofDays(15)),
                DURATION.asLowercaseCql(),
                new CqlDuration[]{ CqlDuration.from("P21W"), CqlDuration.from("P1YT6H30M35S"),
                    CqlDuration.from("PT360H") }
            ),
            Arguments.of(List.of(24.4d, 3, 16.5f, "87.98", Float.valueOf("15.3")), FLOAT.asLowercaseCql(),
                new Float[]{ 24.4f, 3.0f, 16.5f, 87.98f, 15.3f }
            ),
            Arguments.of(List.of("127.0.0.1", "::1"), INET.asLowercaseCql(),
                new InetAddress[]{ InetAddress.getByName("127.0.0.1"), InetAddress.getByName("::1") }
            ),
            Arguments.of(List.of(1, Integer.valueOf("2"), "3", '4'), INT.asLowercaseCql(), new Integer[]{ 1, 2, 3, 4 }),
            Arguments.of(List.of(1, Short.valueOf("2"), "3", '4'), SMALLINT.asLowercaseCql(),
                new Short[]{ 1, 2, 3, 4 }),
            Arguments.of(List.of(Time.valueOf(nowLocalTime), previousHourLocalTime,
                    OffsetTime.ofInstant(nextHourInstant, systemDefault()), twoHoursAgoInstant, sampleTimeAsString),
                TIME.asLowercaseCql(),
                new Time[]{ Time.valueOf(nowLocalTime), Time.valueOf(previousHourLocalTime),
                    Time.valueOf(nextHourLocalTime), Time.valueOf(twoHoursAgoLocalTime),
                    Time.valueOf(sampleTimeAsString) }
            ),
            Arguments.of(List.of(Timestamp.valueOf(nowLocalDateTime), yesterdayLocalDateTime,
                    OffsetDateTime.ofInstant(tomorrowInstant, systemDefault()), nextWeekInstant,
                    new Calendar.Builder().setInstant(nowInstant.toEpochMilli()).build(), sampleDateTimeAsString),
                TIMESTAMP.asLowercaseCql(),
                new Timestamp[]{ Timestamp.valueOf(nowLocalDateTime), Timestamp.valueOf(yesterdayLocalDateTime),
                    Timestamp.valueOf(tomorrowLocalDateTime), Timestamp.valueOf(nextWeekLocalDateTime),
                    Timestamp.valueOf(nowLocalDateTime.truncatedTo(MILLIS)), Timestamp.valueOf(sampleDateTimeAsString) }
            ),
            Arguments.of(List.of(uuid, uuidAsString), TIMEUUID.asLowercaseCql(),
                new UUID[]{ uuid, java.util.UUID.fromString(uuidAsString) }),
            Arguments.of(List.of(uuid, uuidAsString), DataTypeEnum.UUID.cqlType,
                new UUID[]{ uuid, java.util.UUID.fromString(uuidAsString) }),
            Arguments.of(List.of(1, Byte.valueOf("2"), "3", '4'), TINYINT.cqlType, new Byte[]{ 1, 2, 3, 4 }),
            Arguments.of(List.of(Integer.valueOf("36"), 37, "38", new BigInteger("39")), VARINT.cqlType,
                new BigInteger[]{ new BigInteger("36"), new BigInteger("37"), new BigInteger("38"),
                    new BigInteger("39") }
            )
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
    void givenArray_whenGetArrayWithConversionMap_throwSQLFeatureNotSupportedException() throws SQLException {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        assertThrows(SQLFeatureNotSupportedException.class, () -> sut.getArray(Map.of("text", ByteBuffer.class)));
    }

    static Stream<Arguments> buildArrayResultSetTestCases() {
        return Stream.of(
            Arguments.of(SAMPLE_ITEMS, ASCII.asLowercaseCql()),
            Arguments.of(SAMPLE_ITEMS, TEXT.asLowercaseCql()),
            Arguments.of(SAMPLE_ITEMS, VARCHAR.asLowercaseCql()),
            Arguments.of(List.of(10L, 20L, 30L), BIGINT.asLowercaseCql()),
            Arguments.of(List.of(40L, 50L, 60L), COUNTER.asLowercaseCql()),
            Arguments.of(SAMPLE_ITEMS, BLOB.asLowercaseCql()),
            Arguments.of(SAMPLE_ITEMS, CUSTOM.asLowercaseCql()),
            Arguments.of(List.of(true, false, true, true), BOOLEAN.asLowercaseCql()),
            Arguments.of(
                List.of(LocalDate.now(), LocalDate.now().minusDays(1L), LocalDate.now().plusDays(1L)),
                DATE.asLowercaseCql()),
            Arguments.of(List.of(new BigDecimal("4.01"), 5.2, 6.3), DECIMAL.asLowercaseCql()),
            Arguments.of(List.of(1.01d, 2.4d, 3.5d), DOUBLE.asLowercaseCql()),
            Arguments.of(List.of(CqlDuration.from("P1W"), CqlDuration.from("PT3H")), DURATION.asLowercaseCql()),
            Arguments.of(List.of(1.01f, 2.4f, 3.5f), FLOAT.asLowercaseCql()),
            Arguments.of(List.of("127.0.0.1", "::1"), INET.asLowercaseCql()),
            Arguments.of(List.of(1, 2, 3, 4), INT.asLowercaseCql()),
            Arguments.of(List.of(6, 7, 8, 9), SMALLINT.asLowercaseCql()),
            Arguments.of(
                List.of(LocalTime.now(), LocalTime.now().minusHours(1L), LocalTime.now().plusHours(1L)),
                TIME.asLowercaseCql()),
            Arguments.of(
                List.of(LocalDateTime.now(), LocalDateTime.now().minusDays(1L), LocalDateTime.now().plusDays(1L)),
                TIMESTAMP.asLowercaseCql()),
            Arguments.of(List.of(randomUUID(), randomUUID(), randomUUID()), TIMEUUID.asLowercaseCql()),
            Arguments.of(List.of(randomUUID(), randomUUID(), randomUUID()), DataTypeEnum.UUID.asLowercaseCql()),
            Arguments.of(List.of(-1, 0, 1), TINYINT.asLowercaseCql()),
            Arguments.of(List.of(new BigInteger("100"), 200, 300), VARINT.asLowercaseCql())
        );
    }

    @ParameterizedTest
    @MethodSource("buildArrayResultSetTestCases")
    void givenArray_whenGetResultSet_returnExpectedResultSet(final List<Object> data,
                                                             final String cqlType) throws SQLException {
        final var sut = new ArrayImpl(data, cqlType);
        final var rs = sut.getResultSet();
        for (int i = 0; i < data.size(); i++) {
            rs.next();
            assertEquals(i + 1, rs.getInt(1));
            assertEquals(sut.getArray()[i], rs.getObject(2));
        }
    }

    @Test
    void givenArray_whenGetResultSetSlice_returnExpectedResultSet() throws SQLException {
        final var sut = new ArrayImpl(SAMPLE_ITEMS);
        final long startIndex = 3;
        final int count = 2;
        final var rs = sut.getResultSet(startIndex, count);
        for (int i = 0; i < count; i++) {
            rs.next();
            assertEquals(i + (int) startIndex, rs.getInt(1));
            assertEquals(sut.getArray()[i + (int) startIndex - 1], rs.getObject(2));
        }
    }

    @Test
    void givenArray_whenGetResultSetWithConversionMap_throwSQLFeatureNotSupportedException() throws SQLException {
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
