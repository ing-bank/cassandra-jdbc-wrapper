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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.ing.data.cassandra.jdbc.optionset.Default;
import com.ing.data.cassandra.jdbc.types.DataTypeEnum;
import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_4;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_5;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * These tests of non-regression are those existing in the
 * <a href="https://github.com/adejanovski/cassandra-jdbc-wrapper/">original project from GitHub</a>.
 */
class JdbcRegressionUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace3";
    private static final String TABLE = "regressions_test";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");

        // Update cluster name according to the configured name.
        /*
        try (final Statement statement = sqlConnection.createStatement()) {
            final String configuredClusterName = cassandraContainer.getEnvMap().get("CASSANDRA_CLUSTER_NAME");
            statement.execute("UPDATE system.local SET cluster_name = '" + configuredClusterName
                + "' WHERE key = 'local'");
        } catch (final SQLException e) {
            LOG.error("Cannot update cluster_name in system.local table.", e);
        }
        */
    }

    private static CassandraStatementExtras statementExtras(final Statement statement) throws Exception {
        final Class<?> cse = Class.forName("com.ing.data.cassandra.jdbc.CassandraStatementExtras");
        return (CassandraStatementExtras) statement.unwrap(cse);
    }

    @Test
    void testIssue10() throws Exception {
        final String insert = "INSERT INTO regressions_test (keyname, bValue, iValue) VALUES('key0', true, 2000);";
        Statement statement = sqlConnection.createStatement();
        statement.executeUpdate(insert);
        statement.close();

        statement = sqlConnection.createStatement();
        final ResultSet result = statement.executeQuery("SELECT bValue, iValue FROM regressions_test " +
            "WHERE keyname = 'key0';");
        result.next();
        assertTrue(result.getBoolean(1));
        assertEquals(2000, result.getInt(2));
    }

    @Test
    void testIssue18() throws Exception {
        sqlConnection.close();
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");
        final Statement statement = sqlConnection.createStatement();

        final String truncateQuery = "TRUNCATE regressions_test;";
        statement.execute(truncateQuery);

        final String insertQuery1 = "INSERT INTO regressions_test (keyname, bValue, iValue) " +
            "VALUES('key0', true, 2000);";
        statement.executeUpdate(insertQuery1);

        final String insertQuery2 = "INSERT INTO regressions_test (keyname, bValue) VALUES('key1', false);";
        statement.executeUpdate(insertQuery2);

        final String selectQuery = "SELECT * from regressions_test;";
        final ResultSet result = statement.executeQuery(selectQuery);
        ResultSetMetaData metadata = result.getMetaData();

        int colCount = metadata.getColumnCount();
        // Before calling next().
        assertEquals(3, colCount);
        assertEquals("key0", result.getString(1));
        assertTrue(result.getBoolean(2));
        assertEquals(2000, result.getInt(3));

        // Fetching each row with next() call.
        while (result.next()) {
            metadata = result.getMetaData();
            colCount = metadata.getColumnCount();
            assertEquals(3, colCount);

            if (result.getRow() == 1) {
                assertEquals("key0", result.getString(1));
                assertTrue(result.getBoolean(2));
                assertEquals(2000, result.getInt(3));
            } else if (result.getRow() == 2) {
                assertEquals("key1", result.getString(1));
                assertFalse(result.getBoolean(2));
                assertEquals(0, result.getInt(3));
            }
        }
    }

    @Test
    void testIssue33() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        // Create the target column family.
        final String createTableQuery = "CREATE COLUMNFAMILY t33 (keyValue int PRIMARY KEY, col1 text);";

        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");

        // Paraphrase of the snippet from the issue #33 provided test.
        final PreparedStatement stmt2 = sqlConnection.prepareStatement("UPDATE t33 SET col1 = ? WHERE keyValue = 123;");
        stmt2.setString(1, "mark");
        stmt2.executeUpdate();

        final ResultSet result = stmt2.executeQuery("SELECT * FROM t33;");
        final ResultSetMetaData metadata = result.getMetaData();
        final int colCount = metadata.getColumnCount();
        assertEquals(2, colCount);
        assertEquals(123, result.getInt(1));
        assertEquals("mark", result.getString(2));
    }

    @Test
    void testIssue40() throws Exception {
        final DatabaseMetaData md = sqlConnection.getMetaData();

        // Test various retrieval methods.
        ResultSet result = md.getTables(sqlConnection.getCatalog(), null, "%", new String[]{"TABLE"});
        // Make sure we have found a table.
        assertTrue(result.next());
        result = md.getTables(null, KEYSPACE, TABLE, null);
        // Make sure we have found the table asked for.
        assertTrue(result.next());
        result = md.getTables(null, KEYSPACE, TABLE, new String[]{"TABLE"});

        assertTrue(result.next());
        result = md.getTables(sqlConnection.getCatalog(), KEYSPACE, TABLE, new String[]{"TABLE"});

        // Check the table name.
        assertTrue(result.next());
        final String tableName = result.getString("TABLE_NAME");
        assertEquals(TABLE, tableName);

        // Load the columns.
        // Make sure we have found first column.
        result = md.getColumns(sqlConnection.getCatalog(), KEYSPACE, TABLE, null);
        assertTrue(result.next());
        // Make sure table name match.
        assertEquals(TABLE, result.getString("TABLE_NAME"));
        String columnName = result.getString("COLUMN_NAME");
        // Check column names and types.
        assertEquals("keyname", columnName);
        assertEquals(Types.VARCHAR, result.getInt("DATA_TYPE"));

        assertTrue(result.next());
        columnName = result.getString("COLUMN_NAME");
        assertEquals("bvalue", columnName);
        assertEquals(Types.BOOLEAN, result.getInt("DATA_TYPE"));

        assertTrue(result.next());
        columnName = result.getString("COLUMN_NAME");
        assertEquals("ivalue", columnName);
        assertEquals(Types.INTEGER, result.getInt("DATA_TYPE"));

        // Make sure we filter.
        result = md.getColumns(sqlConnection.getCatalog(), KEYSPACE, TABLE, "bvalue");
        result.next();
        assertFalse(result.next());
    }

    @Test
    void testIssue59() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        // Create the target column family.
        final String createTableQuery = "CREATE COLUMNFAMILY t59 (keyValue int PRIMARY KEY, col1 text);";

        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");

        final PreparedStatement stmt2 = sqlConnection.prepareStatement("UPDATE t59 SET col1 = ? WHERE keyValue = 123;",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt2.setString(1, "hello");
        stmt2.executeUpdate();

        final ResultSet result = stmt2.executeQuery("SELECT * FROM t59;");
        final ResultSetMetaData metadata = result.getMetaData();
        final int colCount = metadata.getColumnCount();
        assertEquals(2, colCount);
        assertEquals(123, result.getInt(1));
        assertEquals("hello", result.getString(2));
    }

    @Test
    void testIssue65() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        // Create the target column family.
        final String createTableQuery = "CREATE COLUMNFAMILY t65 (keyValue text PRIMARY KEY, int1 int, int2 int, "
            + "intSet set<int>);";

        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");

        final Statement stmt2 = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO t65 (keyValue, int1, int2, intSet) "
            + "VALUES ('key1', 1, 100, {10,20,30,40});";
        stmt2.executeUpdate(insertQuery);

        ResultSet result = stmt2.executeQuery("SELECT * FROM t65;");
        final ResultSetMetaData metadata = result.getMetaData();
        final int colCount = metadata.getColumnCount();
        assertEquals(4, colCount);
        assertEquals("key1", result.getString(1));
        assertEquals(1, result.getInt(2));
        assertEquals(100, result.getInt(3));
        Set<?> intSet = ((CassandraResultSet) result).getSet(4);
        assertThat(intSet, is(instanceOf(LinkedHashSet.class)));
        assertEquals(4, intSet.size());
        assertTrue(intSet.contains(10));
        assertTrue(intSet.contains(20));
        assertTrue(intSet.contains(30));
        assertTrue(intSet.contains(40));

        final String updateQuery = "UPDATE t65 SET intSet = ? WHERE keyValue = ?;";
        final PreparedStatement stmt3 = sqlConnection.prepareStatement(updateQuery);
        final Set<Integer> mySet = new HashSet<>();
        stmt3.setObject(1, mySet, Types.OTHER);
        stmt3.setString(2, "key1");
        stmt3.executeUpdate();

        result = stmt2.executeQuery("SELECT * FROM t65;");
        intSet = ((CassandraResultSet) result).getSet(4);
        assertThat(intSet, is(instanceOf(LinkedHashSet.class)));
        assertTrue(intSet.isEmpty());
    }

    @Test
    void testIssue71() throws Exception {
        Statement stmt = sqlConnection.createStatement();

        // Create the target Column family
        final String createTableQuery = "CREATE COLUMNFAMILY t71 (keyValue int PRIMARY KEY, col1 text);";

        stmt.execute(createTableQuery);
        stmt.close();

        // At this point consistency level should be set the LOCAL_ONE (default value) in the connection.
        ConsistencyLevel consistencyLevel = statementExtras(stmt).getConsistencyLevel();
        assertEquals(ConsistencyLevel.LOCAL_ONE, consistencyLevel);
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1&consistency=QUORUM");

        // At this point consistency level should be set the QUORUM in the connection.
        stmt = sqlConnection.createStatement();

        consistencyLevel = statementExtras(stmt).getConsistencyLevel();
        assertEquals(ConsistencyLevel.QUORUM, consistencyLevel);
    }

    @Test
    void testIssue74() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        final java.util.Date NOW = new java.util.Date();

        // Create the target column family.
        final String createTableQuery = "CREATE COLUMNFAMILY t74 (id BIGINT PRIMARY KEY, col1 TIMESTAMP);";

        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");

        final String insertQuery = "INSERT INTO t74 (id, col1) VALUES (?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setLong(1, 1L);
        stmt2.setObject(2, new Timestamp(NOW.getTime()), Types.TIMESTAMP);
        stmt2.execute();

        final Statement stmt3 = sqlConnection.createStatement();
        final ResultSet result = stmt3.executeQuery("SELECT * FROM t74;");

        assertTrue(result.next());
        assertEquals(1L, result.getLong(1));
        Timestamp tsValue = result.getTimestamp(2);

        assertEquals(NOW, tsValue);
        tsValue = (Timestamp) result.getObject(2);
        assertEquals(NOW, tsValue);
    }

    @Test
    void testIssue75() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        final String truncateQuery = "TRUNCATE regressions_test;";
        stmt.execute(truncateQuery);

        final String selectQuery = "SELECT iValue FROM " + TABLE;
        final ResultSet result = stmt.executeQuery(selectQuery);
        assertFalse(result.next());
        final ResultSetMetaData rsmd = result.getMetaData();
        assertTrue(rsmd.getColumnDisplaySize(1) != 0);
        assertNotNull(rsmd.getColumnLabel(1));
        stmt.close();
    }

    @Test
    void testIssue76() throws Exception {
        final DatabaseMetaData md = sqlConnection.getMetaData();

        // Make sure we have found an index.
        final ResultSet result = md.getIndexInfo(sqlConnection.getCatalog(), KEYSPACE, TABLE, false, false);
        assertTrue(result.next());

        // Check the column name from index.
        final String columnName = result.getString("COLUMN_NAME");
        assertEquals("ivalue", columnName);
    }

    @Test
    void testIssue77() throws Exception {
        final DatabaseMetaData md = sqlConnection.getMetaData();

        // Make sure we have found a primary key.
        final ResultSet result = md.getPrimaryKeys(sqlConnection.getCatalog(), KEYSPACE, TABLE);
        assertTrue(result.next());

        // Check the column name from index.
        final String columnName = result.getString("COLUMN_NAME");
        assertEquals("keyname", columnName);
    }

    @Test
    void testIssue78() throws Exception {
        final DatabaseMetaData md = sqlConnection.getMetaData();
        // Load the columns, without neither catalog nor schema.
        // Make sure we've found a column.
        final ResultSet result = md.getColumns(null, "%", TABLE, "ivalue");
        assertTrue(result.next());
    }

    // About "OverwrittenKey" warning suppression: we voluntarily use duplicate keys in this test.
    @SuppressWarnings({"OverwrittenKey", "ResultOfMethodCallIgnored"})
    @Test
    void testIssue80() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        final long now = OffsetDateTime.now().toEpochSecond() * 1_000;

        // Create the target table with each basic data type available on Cassandra.
        final String createTableQuery = "CREATE TABLE t80 (bigint_col bigint PRIMARY KEY, ascii_col ascii, "
            + "blob_col blob, boolean_col boolean, decimal_col decimal, double_col double, "
            + "float_col float, inet_col inet, int_col int, text_col text, timestamp_col timestamp, uuid_col uuid, "
            + "timeuuid_col timeuuid, varchar_col varchar, varint_col varint, string_set_col set<text>, "
            + "string_list_col list<text>, string_map_col map<text,text>, date_col date, time_col time, "
            + "smallint_col smallint, tinyint_col tinyint, duration_col duration, url_col text);";

        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");

        final String insertQuery = "INSERT INTO t80(bigint_col, ascii_col, blob_col, boolean_col, decimal_col, "
            + "double_col, float_col, inet_col, int_col, text_col, timestamp_col, uuid_col, timeuuid_col, varchar_col, "
            + "varint_col, string_set_col, string_list_col, string_map_col, date_col, time_col, smallint_col, "
            + "tinyint_col, duration_col, url_col) "
            + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setObject(1, 1L);
        stmt2.setObject(2, "test");
        stmt2.setObject(3, new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8)));
        stmt2.setObject(4, true);
        stmt2.setObject(5, new BigDecimal("5.1"));
        stmt2.setObject(6, 5.1);
        stmt2.setObject(7, (float) 5.1);
        final InetAddress inet = InetAddress.getLocalHost();
        stmt2.setObject(8, inet);
        stmt2.setObject(9, 1);
        stmt2.setObject(10, "test");
        stmt2.setObject(11, new Timestamp(now));
        final UUID uuid = UUID.randomUUID();
        stmt2.setObject(12, uuid);
        stmt2.setObject(13, "test");
        stmt2.setObject(14, 1);
        final HashSet<String> testSet = new HashSet<>();
        testSet.add("test"); // Voluntarily add twice the same value.
        testSet.add("test");
        stmt2.setObject(15, testSet);
        final ArrayList<String> testList = new ArrayList<>();
        testList.add("tes1");
        testList.add("tes2");
        stmt2.setObject(16, testList);
        final HashMap<String, String> testMap = new HashMap<>();
        testMap.put("1", "test");
        testMap.put("2", "test");
        stmt2.setObject(17, testMap);
        stmt2.setObject(18, new Date(now));
        stmt2.setObject(19, new Time(now));
        stmt2.setObject(20, (short) 1);
        stmt2.setObject(21, (byte) 1);
        final CqlDuration testDuration1 = CqlDuration.from("10h15m25s");
        stmt2.setObject(22, testDuration1);
        stmt2.setObject(23, "https://ing.com");
        stmt2.execute();

        stmt2.setLong(1, 2L);
        stmt2.setString(2, "test");
        stmt2.setObject(3, new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8)));
        stmt2.setBoolean(4, true);
        stmt2.setBigDecimal(5, new BigDecimal("5.1"));
        stmt2.setDouble(6, 5.1);
        stmt2.setFloat(7, (float) 5.1);
        stmt2.setObject(8, inet);
        stmt2.setInt(9, 1);
        stmt2.setString(10, "test");
        stmt2.setTimestamp(11, new Timestamp(now));
        stmt2.setObject(12, uuid);
        stmt2.setString(13, "test");
        stmt2.setInt(14, 1);
        stmt2.setObject(15, testSet);
        stmt2.setObject(16, testList);
        stmt2.setObject(17, testMap);
        stmt2.setDate(18, new Date(now));
        stmt2.setTime(19, new Time(now));
        stmt2.setShort(20, (short) 10);
        stmt2.setByte(21, (byte) 2);
        final CqlDuration testDuration2 = CqlDuration.from("9d20h30m11s");
        ((CassandraPreparedStatement) stmt2).setDuration(22, testDuration2);
        final URL testUrl = new URL("https://ing.com");
        stmt2.setURL(23, testUrl);
        stmt2.execute();

        final Statement stmt3 = sqlConnection.createStatement();
        ResultSet result = stmt3.executeQuery("SELECT * FROM t80 WHERE bigint_col = 1;");
        assertTrue(result.next());
        assertEquals(1L, result.getLong("bigint_col"));
        assertEquals("test", result.getString("ascii_col"));
        byte[] array = new byte[result.getBinaryStream("blob_col").available()];
        result.getBinaryStream("blob_col").read(array);
        assertEquals("test", new String(array, StandardCharsets.UTF_8));
        assertTrue(result.getBoolean("boolean_col"));
        assertEquals(new BigDecimal("5.1"), result.getBigDecimal("decimal_col"));
        assertEquals(5.1, result.getDouble("double_col"), 0);
        assertEquals((float) 5.1, result.getFloat("float_col"), 0);
        assertEquals(InetAddress.getLocalHost(), result.getObject("inet_col"));
        assertEquals(1, result.getInt("int_col"));
        assertEquals("test", result.getString("text_col"));
        assertEquals(new Timestamp(now), result.getTimestamp("timestamp_col"));
        // 12 - cannot test timeuuid as it is generated by the server
        assertEquals(uuid, result.getObject("uuid_col"));
        assertEquals("test", result.getString("varchar_col"));
        assertEquals(1, result.getLong("varint_col"));
        Set<?> retSet = ((CassandraResultSet) result).getSet("string_set_col");
        assertTrue(retSet instanceof LinkedHashSet);
        assertEquals(1, retSet.size());
        List<?> retList = ((CassandraResultSet) result).getList("string_list_col");
        assertTrue(retList instanceof ArrayList);
        assertEquals(2, retList.size());
        Map<?, ?> retMap = ((CassandraResultSet) result).getMap("string_map_col");
        assertTrue(retMap instanceof HashMap);
        assertEquals(2, retMap.keySet().size());
        assertEquals(new Date(now).toString(), result.getDate("date_col").toString());
        assertEquals(new Time(now).toString(), result.getTime("time_col").toString());
        assertEquals(1, result.getShort("smallint_col"));
        assertEquals(1, result.getByte("tinyint_col"));
        assertEquals(1, result.getShort("tinyint_col")); // TINYINT could also be retrieved as short.
        assertEquals(testDuration1, ((CassandraResultSet) result).getDuration("duration_col"));
        assertEquals(testUrl, result.getURL("url_col"));

        result = stmt3.executeQuery("SELECT * FROM t80 where bigint_col = 2;");
        assertTrue(result.next());
        assertEquals(2L, result.getLong("bigint_col"));
        assertEquals("test", result.getString("ascii_col"));
        array = new byte[result.getBinaryStream("blob_col").available()];
        result.getBinaryStream("blob_col").read(array);
        assertEquals("test", new String(array, StandardCharsets.UTF_8));
        assertTrue(result.getBoolean("boolean_col"));
        assertEquals(new BigDecimal("5.1"), result.getBigDecimal("decimal_col"));
        assertEquals(5.1, result.getDouble("double_col"), 0);
        assertEquals((float) 5.1, result.getFloat("float_col"), 0);
        assertEquals(InetAddress.getLocalHost(), result.getObject("inet_col"));
        assertEquals(1, result.getInt("int_col"));
        assertEquals("test", result.getString("text_col"));
        assertEquals(new Timestamp(now), result.getTimestamp("timestamp_col"));
        // 12 - cannot test timeuuid as it is generated by the server
        assertEquals(uuid, result.getObject("uuid_col"));
        assertEquals("test", result.getString("varchar_col"));
        assertEquals(1, result.getLong("varint_col"));
        retSet = ((CassandraResultSet) result).getSet("string_set_col");
        assertTrue(retSet instanceof LinkedHashSet);
        assertEquals(1, retSet.size());
        retList = ((CassandraResultSet) result).getList("string_list_col");
        assertTrue(retList instanceof ArrayList);
        assertEquals(2, retList.size());
        retMap = ((CassandraResultSet) result).getMap("string_map_col");
        assertTrue(retMap instanceof HashMap);
        assertEquals(2, retMap.keySet().size());
        assertEquals(new Date(now).toString(), result.getDate("date_col").toString());
        assertEquals(new Time(now).toString(), result.getTime("time_col").toString());
        assertEquals(10, result.getShort("smallint_col"));
        assertEquals(2, result.getByte("tinyint_col"));
        assertEquals(testDuration2, result.getObject("duration_col", CqlDuration.class));
        assertEquals(testUrl, result.getObject("url_col", URL.class));

        stmt2.close();
        stmt3.close();
    }

    @Test
    void testIssue102() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        // Create the target column family.
        final String createTableQuery = "CREATE COLUMNFAMILY t102 (bigint_col bigint PRIMARY KEY, null_int_col int, "
            + "null_bigint_col bigint, not_null_int_col int);";
        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");

        final String insertQuery = "INSERT INTO t102(bigint_col, not_null_int_col) values(?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setObject(1, 1L);
        stmt2.setObject(2, 1);
        stmt2.execute();

        final Statement stmt3 = sqlConnection.createStatement();
        final ResultSet result = stmt3.executeQuery("SELECT * FROM t102 WHERE bigint_col = 1;");

        assertTrue(result.next());
        assertEquals(1L, result.getLong("bigint_col"));
        assertEquals(0L, result.getLong("null_bigint_col"));
        assertTrue(result.wasNull());
        assertEquals(0, result.getInt("null_int_col"));
        assertTrue(result.wasNull());
        assertEquals(1, result.getInt("not_null_int_col"));
        assertFalse(result.wasNull());

        stmt3.close();
        stmt2.close();
    }

    @Test
    void testUDTAndTupleCollections() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        // Create UDT and the target column family.
        final String createUDTQuery = "CREATE TYPE IF NOT EXISTS fieldmap (key text, value text)";
        final String createTableQuery = "CREATE COLUMNFAMILY t_udt_tuple_coll (id bigint PRIMARY KEY, "
            + "field_values set<frozen<fieldmap>>, the_tuple list<frozen<tuple<int, text, float>>>, "
            + "field_values_map map<text,frozen<fieldmap>>, tuple_map map<text,frozen<tuple<int,int>>>);";
        stmt.execute(createUDTQuery);
        stmt.execute(createTableQuery);
        stmt.close();

        final Statement stmt2 = sqlConnection.createStatement();
        final String insertQuery = "INSERT INTO t_udt_tuple_coll(id, field_values, the_tuple, field_values_map, " +
            "tuple_map) values(1, {{key : 'key1', value : 'value1'}, {key : 'key2', value : 'value2'}}, " +
            "[(1, 'midVal1', 1.0), (2, 'midVal2', 2.0)], {'map_key1':{key : 'key1', value : 'value1'}, " +
            "'map_key2':{key : 'key2', value : 'value2'}}, {'tuple1':(1, 2),'tuple2':(2,3)});";
        stmt2.execute(insertQuery);

        final ResultSet result = stmt2.executeQuery("SELECT * FROM t_udt_tuple_coll;");

        assertTrue(result.next());
        assertEquals(1L, result.getLong("id"));
        final Set<?> udtSet = (Set<?>) result.getObject("field_values");
        final List<?> tupleList = (List<?>) result.getObject("the_tuple");
        final Map<?, ?> udtMap = (Map<?, ?>) result.getObject("field_values_map");
        final Map<?, ?> tupleMap = (Map<?, ?>) result.getObject(5);

        int i = 0;
        for (final Object val : udtSet) {
            assertThat(val, is(instanceOf(UdtValue.class)));
            final UdtValue udtValue = (UdtValue) val;
            i++;
            assertEquals(udtValue.getString("key"), "key" + i);
            assertEquals(udtValue.getString("value"), "value" + i);
        }

        i = 0;
        for (final Object val : tupleList) {
            assertThat(val, is(instanceOf(TupleValue.class)));
            final TupleValue tupleValue = (TupleValue) val;
            i++;
            assertEquals(tupleValue.getInt(0), i);
            assertEquals(tupleValue.getString(1), "midVal" + i);
            assertEquals(tupleValue.getFloat(2), (float) i);
        }

        final UdtValue udtVal1 = (UdtValue) udtMap.get("map_key1");
        final UdtValue udtVal2 = (UdtValue) udtMap.get("map_key2");
        assertEquals(udtVal1.getString("key"), "key1");
        assertEquals(udtVal1.getString("value"), "value1");
        assertEquals(udtVal2.getString("key"), "key2");
        assertEquals(udtVal2.getString("value"), "value2");

        final TupleValue tupleVal1 = (TupleValue) tupleMap.get("tuple1");
        final TupleValue tupleVal2 = (TupleValue) tupleMap.get("tuple2");
        assertEquals(tupleVal1.getInt(0), 1);
        assertEquals(tupleVal1.getInt(1), 2);
        assertEquals(tupleVal2.getInt(0), 2);
        assertEquals(tupleVal2.getInt(1), 3);

        stmt2.close();
    }

    @Test
    void testGetLongGetDouble() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        final String createTableQuery = "CREATE COLUMNFAMILY getLongGetDouble(bigint_col bigint PRIMARY KEY, "
            + "int_col int, varint_col varint, float_col float);";
        stmt.execute(createTableQuery);
        stmt.close();

        final String insertQuery = "INSERT INTO getLongGetDouble(bigint_col, int_col, varint_col, float_col) "
            + "VALUES (?,?,?,?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setObject(1, 1L);
        stmt2.setInt(2, 1);
        stmt2.setInt(3, 1);
        stmt2.setFloat(4, (float) 1.1);
        stmt2.execute();

        final Statement stmt3 = sqlConnection.createStatement();
        final ResultSet result = stmt3.executeQuery("SELECT * FROM getLongGetDouble WHERE bigint_col = 1;");
        assertTrue(result.next());
        assertEquals(1L, result.getLong("bigint_col"));
        assertEquals(1L, result.getLong("int_col"));
        assertEquals(1L, result.getLong("varint_col"));
        assertEquals(1.1, result.getDouble("float_col"), 0.1);

        stmt3.close();
        stmt2.close();
    }

    @Test
    void testAsyncQuerySizeLimit() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        final String createTableQuery = "CREATE table test_async_query_size_limit(bigint_col bigint PRIMARY KEY, "
            + "int_col int);";
        stmt.execute(createTableQuery);

        final StringBuilder queries = new StringBuilder();
        for (int i = 0; i < CassandraStatement.MAX_ASYNC_QUERIES * 2; i++) {
            queries.append("INSERT INTO test_async_query_size_limit(bigint_col, int_col) values(").append(i)
                .append(",").append(i).append(");");
        }

        assertThrows(SQLTransientException.class, () -> stmt.execute(queries.toString()));
    }

    @Test
    void testSemiColonSplit() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        final String createTableQuery = "CREATE table test_semicolon(bigint_col bigint PRIMARY KEY, text_value text);";
        stmt.execute(createTableQuery);

        final StringBuilder queries = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            queries.append("INSERT INTO test_semicolon(bigint_col, text_value) values(").append(i).append(",'")
                .append(i).append(";; tptp ;").append(i).append("');");
        }

        stmt.execute(queries.toString());
        final ResultSet result = stmt.executeQuery("SELECT * FROM test_semicolon;");
        int nb = 0;
        while (result.next()) {
            nb++;
        }

        assertEquals(10, nb);
    }

    @Test
    void givenConnectionWithPositiveTimeout_whenIsValid_returnTrue() throws Exception {
        assertTrue(sqlConnection.isValid(3));
    }

    @Test
    void givenConnectionWithNegativeTimeout_whenIsValid_throwException() throws Exception {
        assertThrows(SQLException.class, () -> sqlConnection.isValid(-3));
    }

    @Test
    void testTimestampToLongCodec() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        final java.util.Date now = new java.util.Date();

        final String createTableQuery = "CREATE COLUMNFAMILY testTimestampToLongCodec ("
            + "timestamp_col1 timestamp PRIMARY KEY, timestamp_col2 timestamp, text_value text);";
        stmt.execute(createTableQuery);
        stmt.close();

        final String insertQuery = "INSERT INTO testTimestampToLongCodec (timestamp_col1, timestamp_col2, text_value) "
            + "VALUES (?, ?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setObject(1, now.getTime()); // timestamp as long
        stmt2.setObject(2, new Timestamp(now.getTime())); // timestamp as timestamp
        stmt2.setString(3, "text_value"); // just text value
        stmt2.execute();
        stmt2.close();

        final String selectQuery = "SELECT * FROM testTimestampToLongCodec;";
        final PreparedStatement stmt3 = sqlConnection.prepareStatement(selectQuery);
        final ResultSet resultSet = stmt3.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(resultSet.getLong("timestamp_col1"), now.getTime());
        assertEquals(resultSet.getTimestamp("timestamp_col2"), new Timestamp(now.getTime()));
        assertEquals(resultSet.getString("text_value"), "text_value");
        stmt3.close();
    }

    @Test
    void testSetToNullUnsetParams() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        final String createTableQuery = "CREATE COLUMNFAMILY testSetToNullUnsetParams "
            + "(id int PRIMARY KEY, val1 text, val2 text);";
        stmt.execute(createTableQuery);
        stmt.close();

        final String insertQuery = "INSERT INTO testSetToNullUnsetParams (id, val1, val2) VALUES (?, ?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setInt(1, 1);
        stmt2.setString(2, "val1");
        stmt2.execute();
        stmt2.close();

        final String selectQuery = "SELECT * FROM testSetToNullUnsetParams;";
        final PreparedStatement stmt3 = sqlConnection.prepareStatement(selectQuery);
        final ResultSet resultSet = stmt3.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(resultSet.getInt("id"), 1);
        assertEquals(resultSet.getString("val1"), "val1");
        assertNull(resultSet.getString("val2"));
        stmt3.close();
    }

    @Test
    void testBlob() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        final String createTableQuery = "CREATE COLUMNFAMILY testBlob "
            + "(id int PRIMARY KEY, blob_col1 blob, blob_col2 blob);";
        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");

        final String insertQuery = "INSERT INTO testBlob (id, blob_col1 , blob_col2) VALUES (?, ?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setObject(1, 1);
        stmt2.setBlob(2, new ByteArrayInputStream("test1".getBytes(StandardCharsets.UTF_8)));
        final Blob blob = new javax.sql.rowset.serial.SerialBlob("test2".getBytes(StandardCharsets.UTF_8));
        stmt2.setBlob(3, blob);
        stmt2.execute();

        final ResultSet result = stmt2.executeQuery("SELECT * FROM testBlob WHERE id = 1;");
        assertTrue(result.next());
        assertEquals(1, result.getInt("id"));
        final byte[] array = new byte[result.getBinaryStream("blob_col1").available()];
        result.getBinaryStream("blob_col1").read(array);
        assertEquals("test1", new String(array, StandardCharsets.UTF_8));
        final byte[] array2 = new byte[result.getBinaryStream("blob_col2").available()];
        result.getBinaryStream("blob_col2").read(array2);
        assertEquals("test2", new String(array2, StandardCharsets.UTF_8));

        stmt2.close();
    }

    @Test
    void testOriginalIssue24() throws Exception {
        final String insertQuery = "INSERT INTO regressions_test (keyname, bValue, iValue) VALUES('key24-1', true, 1);";
        final Statement statementInsert = sqlConnection.createStatement();
        assertFalse(statementInsert.execute(insertQuery));
        statementInsert.close();

        final String insertPreparedQuery = "INSERT INTO regressions_test (keyname, bValue, iValue) VALUES (?, ?, ?);";
        final PreparedStatement preparedStatement = sqlConnection.prepareStatement(insertPreparedQuery);
        preparedStatement.setString(1, "key24-2");
        preparedStatement.setBoolean(2, false);
        preparedStatement.setInt(3, 2);
        assertFalse(preparedStatement.execute());
        preparedStatement.close();

        final String selectQuery = "SELECT * FROM regressions_test;";
        final Statement statementSelect = sqlConnection.createStatement();
        assertTrue(statementSelect.execute(selectQuery));
        statementSelect.close();
    }

    @Test
    void testIngIssue10() throws Exception {
        final DatabaseMetaData md = sqlConnection.getMetaData();

        final String COLUMN = "keyname";

        ResultSet result;

        result = md.getColumns(sqlConnection.getCatalog(), KEYSPACE, TABLE, null);
        assertTrue(result.next());
        assertEquals(TABLE, result.getString("TABLE_NAME"));
        // Check column names and types.
        assertEquals(COLUMN, result.getString("COLUMN_NAME"));
        assertEquals(false, result.wasNull());

        // The underlying value of the DECIMAL_DIGITS field is null, so
        // getInt should return 0, but wasNull should be true
        assertEquals(0, result.getInt("DECIMAL_DIGITS"));
        assertEquals(true, result.wasNull());

        // Get another field to ensure wasNull is reset to false
        assertEquals(Types.VARCHAR, result.getInt("DATA_TYPE"));
        assertEquals(false, result.wasNull());

        result.close();
    }

    @Test
    void testIngIssue13_Connection() throws Exception {

        assertTrue(
            sqlConnection.isWrapperFor(Connection.class),
            "Cassandra connection can be assigned to Connection");
        assertTrue(
            sqlConnection.isWrapperFor(AbstractConnection.class),
            "Cassandra connection can be assigned to AbstractConnection");
        assertTrue(
            sqlConnection.isWrapperFor(CassandraConnection.class),
            "Cassandra connection can be assigned to CassandraConnection");

        assertFalse(
            sqlConnection.isWrapperFor(String.class),
            "Cassandra connection cannot be assigned to String");
        assertFalse(
            sqlConnection.isWrapperFor(null),
            "Type not provided for wrapper check");

        assertEquals(
            sqlConnection.unwrap(Connection.class),
            sqlConnection,
            "Cassandra connection can be unwrapped to Connection");
        assertThrows(SQLException.class,
            ()-> sqlConnection.unwrap(String.class),
            "Cassandra connection cannot be unwrapped to String");
    }

    @Test
    void testIngIssue13_Statement() throws Exception {

        try (final Statement statement = sqlConnection.createStatement();) {

            assertTrue(
                statement.isWrapperFor(Statement.class),
                "Cassandra statement can be assigned to Statement");
            assertTrue(
                statement.isWrapperFor(AbstractStatement.class),
                "Cassandra statement can be assigned to AbstractStatement");
            assertTrue(
                statement.isWrapperFor(CassandraStatement.class),
                "Cassandra statement can be assigned to CassandraStatement");

            assertFalse(
                statement.isWrapperFor(String.class),
                "Cassandra statement cannot be assigned to String");
            assertFalse(
                statement.isWrapperFor(null),
                "Type not provided for wrapper check");

            assertEquals(
                statement.unwrap(Statement.class),
                statement,
                "Cassandra statement can be unwrapped to Statement");
            assertThrows(SQLException.class,
                ()-> statement.unwrap(String.class),
                "Cassandra statement cannot be unwrapped to String");
        }
    }

    @Test
    void testIngIssue13_ResultSet() throws Exception {

        try (final Statement statement = sqlConnection.createStatement();
            final ResultSet result = statement.executeQuery(
                    "SELECT bValue, iValue FROM regressions_test " +
                    "WHERE keyname = 'key0';");) {

            assertTrue(
                result.isWrapperFor(ResultSet.class),
                "Cassandra results can be assigned to ResultSet");
            assertTrue(
                result.isWrapperFor(AbstractResultSet.class),
                "Cassandra results can be assigned to AbstractResultSet");
            assertTrue(
                result.isWrapperFor(CassandraResultSet.class),
                "Cassandra results can be assigned to CassandraResultSet");

            assertFalse(
                result.isWrapperFor(String.class),
                "Cassandra results cannot be assigned to String");
            assertFalse(
                result.isWrapperFor(null),
                "Type not provided for wrapper check");

            assertEquals(
                result.unwrap(ResultSet.class),
                result,
                "Cassandra results can be unwrapped to ResultSet");
            assertThrows(SQLException.class,
                ()-> result.unwrap(String.class),
                "Cassandra results cannot be unwrapped to String");
        }
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void testIngIssue33() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        // Create the target table.
        final String createTableQuery = "CREATE TABLE t33_blob (key int PRIMARY KEY, blob_col blob);";
        stmt.execute(createTableQuery);
        stmt.close();
        sqlConnection.close();

        // Open it up again to see the new column family.
        sqlConnection = newConnection(KEYSPACE, "localdatacenter=datacenter1");
        final String insertQuery = "INSERT INTO t33_blob (key, blob_col) VALUES(?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setObject(1, 1);
        stmt2.setObject(2, "test123".getBytes(StandardCharsets.UTF_8));
        stmt2.execute();
        stmt2.setObject(1, 2);
        stmt2.setObject(2, "test456".getBytes(StandardCharsets.UTF_8), Types.VARBINARY);
        stmt2.execute();
        stmt2.setObject(1, 3);
        stmt2.setObject(2, "test789".getBytes(StandardCharsets.UTF_8), Types.LONGVARBINARY);
        stmt2.execute();

        final Statement stmt3 = sqlConnection.createStatement();
        ResultSet result = stmt3.executeQuery("SELECT * FROM t33_blob where key = 1;");
        assertTrue(result.next());
        byte[] array = new byte[result.getBinaryStream("blob_col").available()];
        result.getBinaryStream("blob_col").read(array);
        assertEquals("test123", new String(array, StandardCharsets.UTF_8));

        result = stmt3.executeQuery("SELECT * FROM t33_blob where key = 2;");
        assertTrue(result.next());
        array = new byte[result.getBinaryStream("blob_col").available()];
        result.getBinaryStream("blob_col").read(array);
        assertEquals("test456", new String(array, StandardCharsets.UTF_8));

        result = stmt3.executeQuery("SELECT * FROM t33_blob where key = 3;");
        assertTrue(result.next());
        array = new byte[result.getBinaryStream("blob_col").available()];
        result.getBinaryStream("blob_col").read(array);
        assertEquals("test789", new String(array, StandardCharsets.UTF_8));

        stmt2.close();
        stmt3.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testIngIssue75() throws Exception {
        final Statement stmt = sqlConnection.createStatement();

        // Create the table with complex types.
        final String createTableQuery = "CREATE TABLE t75 (id int PRIMARY KEY, "
            + "complex_type1 list<frozen<map<text, text>>>, "
            + "complex_type2 list<frozen<map<text, list<frozen<map<text, text>>>>>>, "
            + "complex_type3 set<frozen<map<text, set<bigint>>>>);";
        stmt.execute(createTableQuery);
        stmt.close();

        // Insert data into the table.
        final String insertQuery =
            "INSERT INTO t75(id, complex_type1, complex_type2, complex_type3) VALUES(?, ?, ?, ?);";
        final PreparedStatement stmt2 = sqlConnection.prepareStatement(insertQuery);
        stmt2.setObject(1, 1);

        final Map<String, String> map = new HashMap<>();
        map.put("a", "10");
        map.put("b", "20");
        final List<Map<String, String>> list = new ArrayList<>();
        list.add(map);
        stmt2.setObject(2, list);

        final Map<String, List<Map<String, String>>> map2 = new HashMap<>();
        map2.put("c", list);
        final List<Map<String, List<Map<String, String>>>> list2 = new ArrayList<>();
        list2.add(map2);
        stmt2.setObject(3, list2);

        final Map<String, Set<Long>> map3 = new HashMap<>();
        final Set<Long> innerSet = new HashSet<>();
        innerSet.add(10L);
        innerSet.add(15L);
        map3.put("d", innerSet);
        final Set<Map<String, Set<Long>>> outerSet = new HashSet<>();
        outerSet.add(map3);
        stmt2.setObject(4, outerSet);

        stmt2.execute();
        stmt2.close();

        final Statement stmt3 = sqlConnection.createStatement();
        final CassandraResultSet result = (CassandraResultSet) stmt3.executeQuery("SELECT * FROM t75 WHERE id = 1;");

        assertTrue(result.next());
        assertEquals(1, result.getInt("id"));

        final List<Map<String, String>> complexType1ValueAsList =
            (List<Map<String, String>>) result.getList("complex_type1");
        final List<Map<String, String>> complexType1ValueAsObject =
            (List<Map<String, String>>) result.getObject("complex_type1");
        assertThat(complexType1ValueAsList, hasItem(map));
        assertThat(complexType1ValueAsObject, hasItem(map));

        final List<Map<String, List<Map<String, String>>>> complexType2ValueAsList =
            (List<Map<String, List<Map<String, String>>>>) result.getList("complex_type2");
        final List<Map<String, List<Map<String, String>>>> complexType2ValueAsObject =
            (List<Map<String, List<Map<String, String>>>>) result.getObject("complex_type2");
        assertThat(complexType2ValueAsList, hasItem(map2));
        assertThat(complexType2ValueAsObject, hasItem(map2));

        final Set<Map<String, Set<Long>>> complexType3ValueAsSet =
            (Set<Map<String, Set<Long>>>) result.getSet("complex_type3");
        final Set<Map<String, Set<Long>>> complexType3ValueAsObject =
            (Set<Map<String, Set<Long>>>) result.getObject("complex_type3");
        assertThat(complexType3ValueAsSet, hasItem(map3));
        assertThat(complexType3ValueAsObject, hasItem(map3));

        stmt3.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testIngIssue77() throws Exception {
        // Create the table.
        final String createTableQuery = "CREATE TABLE t77 (id int PRIMARY KEY, "
            + "list_ts list<timestamp>, list_dt list<date>, list_time list<time>);";
        final Statement stmt = sqlConnection.createStatement();
        stmt.execute(createTableQuery);
        stmt.close();

        // Insert data into the table.
        // Cassandra doesn't support nanoseconds, so set truncate values to milliseconds.
        final Timestamp tsValue1 = Timestamp.from(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        final Timestamp tsValue2 = Timestamp.from(Instant.now().minusSeconds(120).truncatedTo(ChronoUnit.MILLIS));
        final Date dtValue1 = Date.valueOf(LocalDate.now());
        final Date dtValue2 = Date.valueOf(LocalDate.now().minusYears(3).minusMonths(2).minusDays(1));
        final Time timeValue1 = Time.valueOf(LocalTime.now());
        final Time timeValue2 = Time.valueOf(LocalTime.now().plusHours(2).minusMinutes(15).plusSeconds(30));

        final PreparedStatement ps = sqlConnection.prepareStatement(
            "INSERT INTO t77(id, list_ts, list_dt, list_time) VALUES(1, ?, ?, ?)"
        );
        final List<Timestamp> tsList = Arrays.asList(tsValue1, tsValue2);
        final List<Date> dtList = Arrays.asList(dtValue1, dtValue2);
        final List<Time> timeList = Arrays.asList(timeValue1, timeValue2);
        ps.setObject(1, tsList);
        ps.setObject(2, dtList);
        ps.setObject(3, timeList);
        ps.execute();
        ps.close();

        // Get data from the table.
        final Statement stmt2 = sqlConnection.createStatement();
        final ResultSet resultSet = stmt2.executeQuery("SELECT * FROM t77;");
        resultSet.next();
        final List<Timestamp> resultTsList = (List<Timestamp>) resultSet.getObject("list_ts");
        assertThat(resultTsList, hasItems(tsValue1, tsValue2));
        final List<Date> resultDtList = (List<Date>) resultSet.getObject("list_dt");
        assertThat(resultDtList, hasItems(dtValue1, dtValue2));
        final List<Time> resultTimeList = (List<Time>) resultSet.getObject("list_time");
        assertThat(resultTimeList, hasItems(timeValue1, timeValue2));
        resultSet.close();
        stmt2.close();
    }

    @Test
    void testIngIssue78() throws SQLException {
        final CqlSession cqlSession = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(
                cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort())
            )
            .withLocalDatacenter("datacenter1")
            .build();
        final Connection cassandraConnection = new CassandraConnection(
            cqlSession, KEYSPACE, ConsistencyLevel.LOCAL_ONE, false, new Default(), DEFAULT_NAME
        );
        // The following method call checking if the database is an Amazon Keyspaces instance should not throw
        // an exception when the connection is created from a pre-existing session.
        assertNotNull(cassandraConnection.getMetaData().getSQLKeywords());
    }

    @Test
    public void testIngIssue79() {
        final CassandraDataSource datasource = new CassandraDataSource(
            Collections.singletonList(ContactPoint.of(
                cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort()
            )),
            KEYSPACE
        );
        datasource.setUser(cassandraContainer.getUsername());
        datasource.setPassword(cassandraContainer.getPassword());
        datasource.setLocalDataCenter("datacenter1");
        try {
            datasource.getConnection();
        } catch (final Exception e) {
            fail(e);
        }
        assertEquals(cassandraContainer.getUsername(), datasource.getUser());
        assertEquals(cassandraContainer.getPassword(), datasource.getPassword());
    }

    @Test
    public void testIngIssue80() {
        final CassandraDataSource datasource = new CassandraDataSource(
            Collections.singletonList(ContactPoint.of(
                cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort()
            )),
            KEYSPACE
        );
        datasource.setLocalDataCenter("datacenter1");
        // Test the different types of non-String properties
        datasource.setRequestTimeout(10_000L);      // Long
        datasource.setTcpKeepAliveEnabled(true);    // Boolean
        datasource.setFetchSize(50);                // Integer
        try {
            final CassandraConnection connection = datasource.getConnection();
            final DriverExecutionProfile executionProfile =
                connection.getSession().getContext().getConfig().getDefaultProfile();
            assertEquals(Duration.ofSeconds(10), executionProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT));
            assertTrue(executionProfile.getBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE));
            assertEquals(50, connection.getDefaultFetchSize());
        } catch (final Exception e) {
            fail(e);
        }
    }

    static Stream<Arguments> buildComplianceModeUpdateCountTestCases() {
        return Stream.of(
            Arguments.of("Default", 0),
            Arguments.of("Liquibase", -1)
        );
    }

    @ParameterizedTest
    @MethodSource("buildComplianceModeUpdateCountTestCases")
    public void testIngIssue86(final String complianceMode, final int expectedUpdateCount) {
        final CassandraDataSource datasource = new CassandraDataSource(
            Collections.singletonList(ContactPoint.of(
                cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort()
            )),
            KEYSPACE
        );
        datasource.setLocalDataCenter("datacenter1");
        datasource.setComplianceMode(complianceMode);
        try {
            final CassandraConnection connection = datasource.getConnection();
            final Statement stmt = connection.createStatement();

            // Update query using execute(), getUpdateCount() should return the value defined in the specifications
            // of the used compliance mode.
            stmt.execute("INSERT INTO regressions_test (keyname, iValue) VALUES ('issue86', 1)");
            assertEquals(expectedUpdateCount, stmt.getUpdateCount());

            // Update query using executeUpdate() should return the value defined in the specifications of the used
            // compliance mode.
            stmt.execute("INSERT INTO regressions_test (keyname, iValue) VALUES ('issue86', 2)");
            assertEquals(expectedUpdateCount, stmt.getUpdateCount());
        } catch (final Exception e) {
            fail(e);
        }
    }

}
