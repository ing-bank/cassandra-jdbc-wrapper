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
package com.ing.data.cassandra.jdbc;

import com.datastax.driver.core.utils.UUIDs;
import com.ing.data.cassandra.jdbc.utils.CustomObject;
import com.ing.data.cassandra.jdbc.utils.CustomObjectStringOnly;
import com.ing.data.cassandra.jdbc.utils.JsonResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonSupportUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_json_support";
    private static final Pattern UUID_V1_PATTERN = Pattern.compile(
        "^[0-9A-F]{8}-[0-9A-F]{4}-1[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$", Pattern.CASE_INSENSITIVE);
    private static final Pattern UUID_V4_PATTERN = Pattern.compile(
        "^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$", Pattern.CASE_INSENSITIVE);

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void givenInsertJsonStatement_whenExecute_insertExpectedValues() throws Exception {
        final CassandraPreparedStatement preparedStatement =
            sqlConnection.prepareStatement("INSERT INTO json_test JSON ?;");
        preparedStatement.setJson(1, JsonResult.builder().key(2).textValue("secondRow").build());
        preparedStatement.execute();
        preparedStatement.close();

        final Statement statement = sqlConnection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT JSON * FROM json_test WHERE key = 2;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        final CassandraResultSet cassandraResultSet = (CassandraResultSet) resultSet;
        final JsonResult jsonResult = cassandraResultSet.getObjectFromJson(JsonResult.class);
        assertEquals(2, jsonResult.getKey());
        assertEquals("secondRow", jsonResult.getTextValue());

        statement.close();
    }

    @Test
    void givenInsertStatementUsingFromJsonFunction_whenExecute_insertExpectedValues() throws Exception {
        final LocalDate nowDate = LocalDate.now();
        final LocalTime nowTime = LocalTime.now();
        final OffsetDateTime nowDateTime = OffsetDateTime.of(nowDate, nowTime, ZoneOffset.UTC);

        final CassandraPreparedStatement preparedStatement = sqlConnection.prepareStatement(
                "INSERT INTO json_test (key, textValue, customObject) VALUES (?, ?, fromJson(?));");
        preparedStatement.setInt(1, 3);
        preparedStatement.setString(2, "thirdRow");
        preparedStatement.setJson(3, CustomObject.builder()
            .asciiValue("ascii example")
            .bigintValue(987654321L)
            .blobValue(ByteBuffer.wrap("this is an example".getBytes(StandardCharsets.UTF_8)))
            .boolValue(false)
            .dateValue(nowDate)
            .decimalValue(BigDecimal.valueOf(18.97))
            .doubleValue(2345.6)
            .floatValue(21.3f)
            .inetValue(InetAddress.getByName("127.0.0.1"))
            .intValue(1024)
            .listValue(Arrays.asList(1, 1, 2, 4))
            .mapValue(new HashMap<Integer, String>() {{
                put(6, "six");
                put(12, "twelve");
            }})
            .smallintValue((short) 2)
            .setValue(new HashSet<Integer>() {{
                add(2);
                add(3);
                add(5);
            }})
            .textValue("example text")
            .timeValue(nowTime)
            .timeuuidValue(UUIDs.timeBased())
            .tsValue(nowDateTime)
            .tinyintValue((byte) 12)
            .tupleValue(Arrays.asList("10", "ten"))
            .uuidValue(UUIDs.random())
            .varcharValue("varchar example")
            .varintValue(BigInteger.valueOf(987123L))
            .build());
        preparedStatement.execute();
        preparedStatement.close();

        final Statement statement = sqlConnection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT JSON * FROM json_test WHERE key = 3;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        final CassandraResultSet cassandraResultSet = (CassandraResultSet) resultSet;
        final JsonResult jsonResult = cassandraResultSet.getObjectFromJson(JsonResult.class);
        assertEquals(3, jsonResult.getKey());
        assertEquals("thirdRow", jsonResult.getTextValue());
        final CustomObject customObject = jsonResult.getCustomObject();
        assertNotNull(customObject);
        assertEquals("ascii example", customObject.getAsciiValue());
        assertEquals(987_654_321L, customObject.getBigintValue());
        assertNotNull(customObject.getBlobValue());
        assertEquals("this is an example", new String(customObject.getBlobValue().array(), StandardCharsets.UTF_8));
        assertFalse(customObject.getBoolValue());
        assertEquals(nowDate, customObject.getDateValue());
        assertEquals(BigDecimal.valueOf(18.97), customObject.getDecimalValue());
        assertEquals(2345.6, customObject.getDoubleValue());
        assertEquals(21.3f, customObject.getFloatValue());
        assertEquals("127.0.0.1", customObject.getInetValue().getHostAddress());
        assertEquals(1024, customObject.getIntValue());
        assertNotNull(customObject.getListValue());
        assertEquals(1, customObject.getListValue().get(0));
        assertEquals(1, customObject.getListValue().get(1));
        assertEquals(2, customObject.getListValue().get(2));
        assertEquals(4, customObject.getListValue().get(3));
        assertNotNull(customObject.getMapValue());
        assertEquals("six", customObject.getMapValue().get(6));
        assertEquals("twelve", customObject.getMapValue().get(12));
        assertEquals((short) 2, customObject.getSmallintValue());
        assertNotNull(customObject.getSetValue());
        assertThat(customObject.getSetValue(), contains(2, 3, 5));
        assertEquals("example text", customObject.getTextValue());
        assertEquals(nowTime, customObject.getTimeValue());
        assertEquals(nowDateTime.truncatedTo(ChronoUnit.MILLIS), customObject.getTsValue());
        assertThat(customObject.getTimeuuidValue().toString(), matchesPattern(UUID_V1_PATTERN));
        assertEquals((byte) 12, customObject.getTinyintValue());
        assertNotNull(customObject.getTupleValue());
        assertEquals("10", customObject.getTupleValue().get(0));
        assertEquals("ten", customObject.getTupleValue().get(1));
        assertThat(customObject.getUuidValue().toString(), matchesPattern(UUID_V4_PATTERN));
        assertEquals("varchar example", customObject.getVarcharValue());
        assertEquals(BigInteger.valueOf(987123L), customObject.getVarintValue());

        statement.close();
    }

    @Test
    void givenSelectJsonStatementWithAllColumns_whenExecute_getExpectedResultSet() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final ResultSet resultSet = statement.executeQuery("SELECT JSON * FROM json_test WHERE key = 1;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        final CassandraResultSet cassandraResultSet = (CassandraResultSet) resultSet;
        final JsonResult jsonResult = cassandraResultSet.getObjectFromJson(JsonResult.class);
        assertEquals(1, jsonResult.getKey());
        assertEquals("firstRow", jsonResult.getTextValue());
        final CustomObject customObject = jsonResult.getCustomObject();
        assertEquals("ascii text", customObject.getAsciiValue());
        assertEquals(12_345_678_900_000L, customObject.getBigintValue());
        assertNotNull(customObject.getBlobValue());
        assertEquals("this is a blob", new String(customObject.getBlobValue().array(), StandardCharsets.UTF_8));
        assertTrue(customObject.getBoolValue());
        final LocalDate expectedLocalDate = LocalDate.of(2023, Month.MARCH, 25);
        assertEquals(expectedLocalDate, customObject.getDateValue());
        assertEquals(BigDecimal.valueOf(18.97), customObject.getDecimalValue());
        assertEquals(2345.6, customObject.getDoubleValue());
        assertEquals(21.3f, customObject.getFloatValue());
        assertEquals("127.0.0.1", customObject.getInetValue().getHostAddress());
        assertEquals(98, customObject.getIntValue());
        assertNotNull(customObject.getListValue());
        assertEquals(4, customObject.getListValue().get(0));
        assertEquals(6, customObject.getListValue().get(1));
        assertEquals(10, customObject.getListValue().get(2));
        assertNotNull(customObject.getMapValue());
        assertEquals("three", customObject.getMapValue().get(3));
        assertEquals("eight", customObject.getMapValue().get(8));
        assertEquals((short) 2, customObject.getSmallintValue());
        assertNotNull(customObject.getSetValue());
        assertThat(customObject.getSetValue(), contains(2, 3, 5));
        assertEquals("simple text", customObject.getTextValue());
        final LocalTime expectedLocalTime = LocalTime.of(12, 30, 45, 789_000_000);
        assertEquals(expectedLocalTime, customObject.getTimeValue());
        assertEquals(OffsetDateTime.of(expectedLocalDate, expectedLocalTime, ZoneOffset.UTC),
            customObject.getTsValue());
        assertThat(customObject.getTimeuuidValue().toString(), matchesPattern(UUID_V1_PATTERN));
        assertEquals((byte) 12, customObject.getTinyintValue());
        assertNotNull(customObject.getTupleValue());
        assertEquals("5", customObject.getTupleValue().get(0));
        assertEquals("five", customObject.getTupleValue().get(1));
        assertThat(customObject.getUuidValue().toString(), matchesPattern(UUID_V4_PATTERN));
        assertEquals("varchar text", customObject.getVarcharValue());
        assertEquals(BigInteger.valueOf(4321), customObject.getVarintValue());

        statement.close();
    }

    @Test
    void givenSelectJsonStatementWithAllColumnsAndTargetObjectUsingOnlyStrings_whenExecute_getExpectedResultSet()
        throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final ResultSet resultSet = statement.executeQuery(
            "SELECT toJson(customObject) AS customObjectStringOnly FROM json_test WHERE key = 1;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        final CassandraResultSet cassandraResultSet = (CassandraResultSet) resultSet;
        final CustomObjectStringOnly customObject = cassandraResultSet.getObjectFromJson("customObjectStringOnly",
            CustomObjectStringOnly.class);
        assertNotNull(customObject);
        assertEquals("ascii text", customObject.getAsciiValue());
        assertEquals("12345678900000", customObject.getBigintValue());
        assertEquals("0x74686973206973206120626c6f62", customObject.getBlobValue());
        assertEquals("true", customObject.getBoolValue());
        assertEquals("2023-03-25", customObject.getDateValue());
        assertEquals("18.97", customObject.getDecimalValue());
        assertEquals("2345.6", customObject.getDoubleValue());
        assertEquals("21.3", customObject.getFloatValue());
        assertEquals("127.0.0.1", customObject.getInetValue());
        assertEquals("98", customObject.getIntValue());
        assertNotNull(customObject.getListValue());
        assertEquals("4", customObject.getListValue().get(0));
        assertEquals("6", customObject.getListValue().get(1));
        assertEquals("10", customObject.getListValue().get(2));
        assertNotNull(customObject.getMapValue());
        assertEquals("three", customObject.getMapValue().get("3"));
        assertEquals("eight", customObject.getMapValue().get("8"));
        assertEquals("2", customObject.getSmallintValue());
        assertNotNull(customObject.getSetValue());
        assertEquals("2", customObject.getSetValue().get(0));
        assertEquals("3", customObject.getSetValue().get(1));
        assertEquals("5", customObject.getSetValue().get(2));
        assertEquals("simple text", customObject.getTextValue());
        assertEquals("12:30:45.789000000", customObject.getTimeValue());
        assertEquals("2023-03-25 12:30:45.789Z", customObject.getTsValue());
        assertThat(customObject.getTimeuuidValue(), matchesPattern(UUID_V1_PATTERN));
        assertEquals("12", customObject.getTinyintValue());
        assertNotNull(customObject.getTupleValue());
        assertEquals("5", customObject.getTupleValue().get(0));
        assertEquals("five", customObject.getTupleValue().get(1));
        assertThat(customObject.getUuidValue(), matchesPattern(UUID_V4_PATTERN));
        assertEquals("varchar text", customObject.getVarcharValue());
        assertEquals("4321", customObject.getVarintValue());

        statement.close();
    }

    @Test
    void givenSelectJsonStatementWithSpecificColumns_whenExecute_getExpectedResultSet() throws Exception {
        final Statement statement = sqlConnection.createStatement();

        final ResultSet resultSet = statement.executeQuery("SELECT JSON key, textValue FROM json_test WHERE key = 1;");
        resultSet.next();

        assertThat(resultSet, is(instanceOf(CassandraResultSet.class)));
        final CassandraResultSet cassandraResultSetPartial = (CassandraResultSet) resultSet;
        final JsonResult jsonResultNoCustomObject = cassandraResultSetPartial.getObjectFromJson(JsonResult.class);
        assertEquals(1, jsonResultNoCustomObject.getKey());
        assertEquals("firstRow", jsonResultNoCustomObject.getTextValue());
        assertNull(jsonResultNoCustomObject.getCustomObject());

        statement.close();
    }

}
