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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import com.ing.data.cassandra.jdbc.types.JdbcAscii;
import com.ing.data.cassandra.jdbc.types.JdbcBoolean;
import com.ing.data.cassandra.jdbc.types.JdbcInt32;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreparedStatementsUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_prep_stmt";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void givenPreparedStatement_whenGetParameterMetaData_returnExpectedMetadataResultSet() throws SQLException {
        final String cql = "SELECT keyname FROM cf_test_ps WHERE t1bValue = ? AND t1iValue = ? ALLOW FILTERING";
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(cql);
        prepStatement.setBoolean(1, true);
        prepStatement.setInt(2, 0);
        final ParameterMetaData parameterMetaData = prepStatement.getParameterMetaData();
        assertNotNull(parameterMetaData);
        assertEquals(2, parameterMetaData.getParameterCount());

        // First parameter: boolean value
        assertEquals(ParameterMetaData.parameterModeIn, parameterMetaData.getParameterMode(1));
        assertEquals(Types.BOOLEAN, parameterMetaData.getParameterType(1));
        assertEquals(DataTypeEnum.BOOLEAN.asLowercaseCql(), parameterMetaData.getParameterTypeName(1));
        assertEquals(Boolean.class.getName(), parameterMetaData.getParameterClassName(1));
        assertEquals(JdbcBoolean.INSTANCE.getPrecision(null), parameterMetaData.getPrecision(1));
        assertEquals(JdbcBoolean.INSTANCE.getScale(null), parameterMetaData.getScale(1));

        // Second parameter: integer value
        assertEquals(ParameterMetaData.parameterModeIn, parameterMetaData.getParameterMode(2));
        assertEquals(Types.INTEGER, parameterMetaData.getParameterType(2));
        assertEquals(DataTypeEnum.INT.asLowercaseCql(), parameterMetaData.getParameterTypeName(2));
        assertEquals(Integer.class.getName(), parameterMetaData.getParameterClassName(2));
        assertEquals(JdbcInt32.INSTANCE.getPrecision(null), parameterMetaData.getPrecision(2));
        assertEquals(JdbcInt32.INSTANCE.getScale(null), parameterMetaData.getScale(2));
    }

    @Test
    void givenSelectPreparedStatement_whenExecuteReturnsNoRows_returnTrue() throws SQLException {
        final String cql = "SELECT keyname FROM cf_test_ps WHERE t1iValue = ? ALLOW FILTERING";
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(cql);
        prepStatement.setInt(1, 99999);
        boolean isResultSet = prepStatement.execute();
        assertTrue(isResultSet);
    }

    @Test
    void givenUpdatePreparedStatement_whenExecuteReturnsNoRows_returnFalse() throws SQLException {
        final String cql = "UPDATE cf_test_ps SET t1iValue = 1 WHERE keyname = ?";
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(cql);
        prepStatement.setString(1, "testRow2");
        boolean isResultSet = prepStatement.execute();
        assertFalse(isResultSet);
    }

    @Test
    void givenPreparedStatement_whenGetResultSetMetaData_returnExpectedMetadataResultSet() throws SQLException {
        final String cql = "SELECT keyname AS resKeyname, t1iValue FROM cf_test_ps WHERE t1bValue = ?";
        final CassandraPreparedStatement prepStatement = sqlConnection.prepareStatement(cql);
        prepStatement.setBoolean(1, true);
        boolean isResultSet = prepStatement.execute();
        assertTrue(isResultSet);
        final ResultSetMetaData rsMetaData = prepStatement.getMetaData();
        assertNotNull(rsMetaData);
        assertEquals(2, rsMetaData.getColumnCount());

        // First column: string value
        assertEquals("reskeyname", rsMetaData.getColumnName(1));
        assertEquals("reskeyname", rsMetaData.getColumnLabel(1));
        assertEquals(String.class.getName(), rsMetaData.getColumnClassName(1));
        assertEquals(DataTypeEnum.TEXT.name(), rsMetaData.getColumnTypeName(1));
        assertEquals(Types.VARCHAR, rsMetaData.getColumnType(1));
        assertEquals(JdbcAscii.INSTANCE.getPrecision(null), rsMetaData.getColumnDisplaySize(1));
        assertEquals(JdbcAscii.INSTANCE.getPrecision(null), rsMetaData.getPrecision(1));
        assertEquals(JdbcAscii.INSTANCE.getScale(null), rsMetaData.getScale(1));
        assertEquals("cf_test_ps", rsMetaData.getTableName(1));
        assertEquals("test_prep_stmt", rsMetaData.getSchemaName(1));
        assertEquals("embedded_test_cluster", rsMetaData.getCatalogName(1));

        // Second column: integer value
        assertEquals("t1ivalue", rsMetaData.getColumnName(2));
        assertEquals("t1ivalue", rsMetaData.getColumnLabel(2));
        assertEquals(Integer.class.getName(), rsMetaData.getColumnClassName(2));
        assertEquals(DataTypeEnum.INT.name(), rsMetaData.getColumnTypeName(2));
        assertEquals(Types.INTEGER, rsMetaData.getColumnType(2));
        assertEquals(JdbcInt32.INSTANCE.getPrecision(null), rsMetaData.getColumnDisplaySize(2));
        assertEquals(JdbcInt32.INSTANCE.getPrecision(null), rsMetaData.getPrecision(2));
        assertEquals(JdbcInt32.INSTANCE.getScale(null), rsMetaData.getScale(2));
        assertEquals("cf_test_ps", rsMetaData.getTableName(2));
        assertEquals("test_prep_stmt", rsMetaData.getSchemaName(2));
        assertEquals("embedded_test_cluster", rsMetaData.getCatalogName(2));
    }

    @Test
    void givenPreparedStatement_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO test_ps_othertypes "
            + "(col_key, col_tuple, col_inet, col_duration, col_uuid)"
            + "VALUES (?, ?, ?, ?, ?);";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(insertQuery);
        preparedStatement.setString(1, "key1");
        final TupleValue aTuple = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.TEXT).newValue("val1", "val2");
        preparedStatement.setObject(2, aTuple, Types.OTHER);
        preparedStatement.setObject(3, InetAddress.getByName("127.0.0.1"), Types.OTHER);
        preparedStatement.setObject(4, CqlDuration.from("15s"), Types.OTHER);
        final UUID generatedUuid = Uuids.random();
        preparedStatement.setObject(5, generatedUuid, Types.OTHER);
        preparedStatement.execute();

        final ResultSet resultSet = statement.executeQuery("SELECT * FROM test_ps_othertypes WHERE col_key = 'key1';");
        resultSet.next();

        assertEquals(aTuple, resultSet.getObject("col_tuple"));
        assertEquals(InetAddress.getByName("127.0.0.1"), resultSet.getObject("col_inet"));
        assertEquals(CqlDuration.from("15s"), resultSet.getObject("col_duration"));
        assertEquals(generatedUuid, resultSet.getObject("col_uuid"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void givenPreparedStatementWithBlobs_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO test_ps_blobs (col_key, col_blob) VALUES (?, ?);";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(insertQuery);
        preparedStatement.setString(1, "key1");
        preparedStatement.setObject(2, new SerialBlob("testJavaSqlBlob".getBytes(StandardCharsets.UTF_8)));
        preparedStatement.execute();

        preparedStatement.setString(1, "key2");
        preparedStatement.setObject(2, new SerialClob("testJavaSqlClob-with accents: Äèéî".toCharArray()));
        preparedStatement.execute();

        preparedStatement.setString(1, "key3");
        preparedStatement.setObject(2, new SerialClob("testJavaSqlNClob".toCharArray()), Types.NCLOB);
        preparedStatement.execute();

        ResultSet resultSet = statement.executeQuery("SELECT * FROM test_ps_blobs WHERE col_key = 'key1';");
        assertTrue(resultSet.next());
        byte[] array = new byte[resultSet.getBinaryStream("col_blob").available()];
        resultSet.getBinaryStream("col_blob").read(array);
        assertEquals("testJavaSqlBlob", new String(array, StandardCharsets.UTF_8));

        resultSet = statement.executeQuery("SELECT * FROM test_ps_blobs WHERE col_key = 'key2';");
        assertTrue(resultSet.next());
        array = new byte[resultSet.getBinaryStream("col_blob").available()];
        resultSet.getBinaryStream("col_blob").read(array);
        assertEquals("testJavaSqlClob-with accents: Äèéî", new String(array, StandardCharsets.UTF_8));

        resultSet = statement.executeQuery("SELECT * FROM test_ps_blobs WHERE col_key = 'key3';");
        assertTrue(resultSet.next());
        array = new byte[resultSet.getBinaryStream("col_blob").available()];
        resultSet.getBinaryStream("col_blob").read(array);
        assertEquals("testJavaSqlNClob", new String(array, StandardCharsets.UTF_8));
    }

    @Test
    void givenPreparedStatementWithUrlAndVarcharTypes_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO test_ps_texts (col_key, col_text) VALUES (?, ?);";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(insertQuery);
        preparedStatement.setObject(1, "key1");
        preparedStatement.setObject(2, new URL("https://cassandra.apache.org/"));
        preparedStatement.execute();

        preparedStatement.setObject(1, "key2");
        preparedStatement.setObject(2, "longvarchar", Types.LONGVARCHAR);
        preparedStatement.execute();

        preparedStatement.setObject(1, "key3");
        preparedStatement.setObject(2, "c", Types.NCHAR);
        preparedStatement.execute();

        preparedStatement.setObject(1, "key4");
        preparedStatement.setObject(2, "nvarchar", Types.NVARCHAR);
        preparedStatement.execute();

        preparedStatement.setObject(1, "key5");
        preparedStatement.setObject(2, "longnvarchar", Types.LONGNVARCHAR);
        preparedStatement.execute();

        ResultSet resultSet = statement.executeQuery("SELECT * FROM test_ps_texts WHERE col_key = 'key1';");
        assertTrue(resultSet.next());
        assertEquals("https://cassandra.apache.org/", resultSet.getString("col_text"));
        resultSet = statement.executeQuery("SELECT * FROM test_ps_texts WHERE col_key = 'key2';");
        assertTrue(resultSet.next());
        assertEquals("longvarchar", resultSet.getString("col_text"));
        resultSet = statement.executeQuery("SELECT * FROM test_ps_texts WHERE col_key = 'key3';");
        assertTrue(resultSet.next());
        assertEquals("c", resultSet.getString("col_text"));
        resultSet = statement.executeQuery("SELECT * FROM test_ps_texts WHERE col_key = 'key4';");
        assertTrue(resultSet.next());
        assertEquals("nvarchar", resultSet.getString("col_text"));
        resultSet = statement.executeQuery("SELECT * FROM test_ps_texts WHERE col_key = 'key5';");
        assertTrue(resultSet.next());
        assertEquals("longnvarchar", resultSet.getString("col_text"));
    }

    @Test
    void givenPreparedStatementWithBitType_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO cf_test_ps (keyname, t1bValue) VALUES (?, ?);";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(insertQuery);
        preparedStatement.setObject(1, "key1");
        preparedStatement.setObject(2, false, Types.BIT);
        preparedStatement.execute();

        preparedStatement.setObject(1, "key2");
        preparedStatement.setObject(2, true, Types.BIT);
        preparedStatement.execute();

        ResultSet resultSet = statement.executeQuery("SELECT * FROM cf_test_ps WHERE keyname = 'key1';");
        assertTrue(resultSet.next());
        assertFalse(resultSet.getBoolean("t1bValue"));

        resultSet = statement.executeQuery("SELECT * FROM cf_test_ps WHERE keyname = 'key2';");
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean("t1bValue"));
    }

    @Test
    void givenPreparedStatementWithDateTimes_whenExecute_insertExpectedValues() throws Exception {
        final Statement statement = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO test_ps_datetimes (col_key, col_time, col_date, col_ts) "
            + "VALUES (?, ?, ?, ?);";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(insertQuery);
        preparedStatement.setObject(1, "key1");
        preparedStatement.setObject(2, LocalTime.of(15, 35, 40, 123456789));
        preparedStatement.setObject(3, LocalDate.of(2023, Month.OCTOBER, 31));
        preparedStatement.setObject(4, LocalDateTime.of(2023, Month.OCTOBER, 31, 16, 40, 25, 123456789));
        preparedStatement.execute();

        final OffsetDateTime testOffsetDateTime = OffsetDateTime.of(2023, 10, 31, 16, 40, 25, 123456789, UTC);
        final long testDateTimeInMillis = testOffsetDateTime.toInstant().toEpochMilli();
        preparedStatement.setObject(1, "key2");
        preparedStatement.setObject(2, OffsetTime.of(15, 35, 40, 123456789, UTC));
        preparedStatement.setObject(3, LocalDate.now());
        preparedStatement.setObject(4, testOffsetDateTime);
        preparedStatement.execute();

        preparedStatement.setObject(1, "key3");
        preparedStatement.setObject(2, LocalTime.now());
        preparedStatement.setObject(3, LocalDate.now());
        preparedStatement.setObject(4, new java.util.Date(testDateTimeInMillis));
        preparedStatement.execute();

        preparedStatement.setObject(1, "key4");
        preparedStatement.setObject(2, LocalTime.now());
        preparedStatement.setObject(3, LocalDate.now());
        preparedStatement.setObject(4, new Calendar.Builder().setInstant(testDateTimeInMillis).build());
        preparedStatement.execute();

        ResultSet resultSet = statement.executeQuery("SELECT * FROM test_ps_datetimes WHERE col_key = 'key1';");
        assertTrue(resultSet.next());
        // Note: Cassandra max precision for timestamps is milliseconds, not nanoseconds
        assertEquals(Time.valueOf(LocalTime.of(15, 35, 40, 123000000)), resultSet.getTime("col_time"));
        assertEquals(Date.valueOf(LocalDate.of(2023, Month.OCTOBER, 31)), resultSet.getDate("col_date"));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2023, Month.OCTOBER, 31, 16, 40, 25, 123000000)),
            resultSet.getTimestamp("col_ts"));

        final Timestamp expectedTimestamp = Timestamp.valueOf(
            OffsetDateTime.of(2023, 10, 31, 16, 40, 25, 123000000, UTC)
            .atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime());
        resultSet = statement.executeQuery("SELECT * FROM test_ps_datetimes WHERE col_key = 'key2';");
        assertTrue(resultSet.next());
        assertEquals(Time.valueOf(OffsetTime.of(15, 35, 40, 123000000, UTC).toLocalTime()),
            resultSet.getTime("col_time"));
        assertEquals(expectedTimestamp, resultSet.getTimestamp("col_ts"));

        resultSet = statement.executeQuery("SELECT * FROM test_ps_datetimes WHERE col_key = 'key3';");
        assertTrue(resultSet.next());
        assertEquals(expectedTimestamp, resultSet.getTimestamp("col_ts"));

        resultSet = statement.executeQuery("SELECT * FROM test_ps_datetimes WHERE col_key = 'key4';");
        assertTrue(resultSet.next());
        assertEquals(expectedTimestamp, resultSet.getTimestamp("col_ts"));
    }

    @Test
    void givenPreparedStatement_whenCloseOnCompletion_statementIsFlaggedAsExpected() throws Exception {
        final Statement statement = sqlConnection.createStatement();
        assertFalse(statement.isCloseOnCompletion());
        statement.closeOnCompletion();
        assertTrue(statement.isCloseOnCompletion());
    }
}
