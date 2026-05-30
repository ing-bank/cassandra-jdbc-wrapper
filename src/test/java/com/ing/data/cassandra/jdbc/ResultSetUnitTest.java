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

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.FORWARD_ONLY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WAS_CLOSED_RS;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test Cassandra Result Sets
 */
class ResultSetUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1");
    }

    @Test
    void givenResultSetWithRows_whenFindColumns_returnExpectedIndex() throws Exception {
        final String cql = "SELECT keyname, t1iValue FROM cf_test1";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertEquals(1, rs.findColumn("keyname"));
        assertEquals(2, rs.findColumn("t1iValue"));
        final SQLSyntaxErrorException exception = assertThrows(SQLSyntaxErrorException.class,
            () -> rs.findColumn("t1bValue"));
        assertEquals("Name provided was not in the list of valid column labels: t1bValue", exception.getMessage());
    }

    @Test
    void givenResultSetWithoutRows_whenFindColumns_returnExpectedIndex() throws Exception {
        final String cql = "SELECT keyname, t2iValue FROM cf_test2";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertEquals(1, rs.findColumn("keyname"));
        assertEquals(2, rs.findColumn("t2iValue"));
        final SQLSyntaxErrorException exception = assertThrows(SQLSyntaxErrorException.class,
            () -> rs.findColumn("t2bValue"));
        assertEquals("Name provided was not in the list of valid column labels: t2bValue", exception.getMessage());
    }

    @Test
    void givenIncompleteResultSet_whenFindColumns_throwException() throws SQLException {
        final CassandraResultSet rs = new CassandraResultSet();
        final SQLSyntaxErrorException exception = assertThrows(SQLSyntaxErrorException.class,
            () -> rs.findColumn("keyname"));
        assertEquals("Name provided was not in the list of valid column labels: keyname", exception.getMessage());
    }

    @Test
    void givenSelectStatementGeneratingWarning_whenGetWarnings_returnExpectedWarning() throws Exception {
        final CassandraStatement mockStmt = mock(CassandraStatement.class);
        final com.datastax.oss.driver.api.core.cql.ResultSet mockDriverRs =
            mock(com.datastax.oss.driver.api.core.cql.ResultSet.class);
        when(mockDriverRs.getExecutionInfos()).thenReturn(Collections.singletonList(mock(ExecutionInfo.class)));
        when(mockDriverRs.getExecutionInfo()).thenReturn(mock(ExecutionInfo.class));
        when(mockDriverRs.getExecutionInfo().getWarnings())
            .thenReturn(Arrays.asList("First warning message", "Second warning message"));
        when(mockDriverRs.iterator()).thenReturn(Collections.emptyIterator());
        final ResultSet fakeRs = new CassandraResultSet(mockStmt, mockDriverRs);
        when(mockStmt.executeQuery(anyString())).thenReturn(fakeRs);

        final ResultSet resultSet = mockStmt.executeQuery("SELECT * FROM test_table");
        assertEquals("First warning message", resultSet.getWarnings().getMessage());
        final SQLWarning nextWarning = resultSet.getWarnings().getNextWarning();
        assertNotNull(nextWarning);
        assertEquals("Second warning message", nextWarning.getMessage());
    }

    @Test
    void givenResultSetWithRows_whenGetObjectAsCalendar_returnExpectedValue() throws Exception {
        final String cql = "SELECT col_ts FROM tbl_test_timestamps WHERE keyname = 'key1'";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        assertEquals(new Calendar.Builder()
                .setInstant(OffsetDateTime.parse("2023-11-01T11:30:25.789+01:00").toEpochSecond())
                .build(), rs.getObject("col_ts", Calendar.class));
    }

    @Test
    void givenTimestampColumn_whenFetchingMetadata_columnScaleShouldBeCorrect() throws Exception {
        final String cql = "select (timestamp) null from system.local";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        int scale = rs.getMetaData().getScale(1);
        assertEquals(3, scale);
    }

    @Test
    void givenResultSetWithRows_whenGetClob_returnExpectedValue() throws Exception {
        final String cql = "SELECT col_blob FROM tbl_test_blobs WHERE keyname = 'key1'";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        final byte[] byteArray = IOUtils.toByteArray(rs.getClob("col_blob").getCharacterStream(),
            StandardCharsets.UTF_8);
        assertArrayEquals("testValueAsClobInUtf8 with accents: Äîéè".getBytes(StandardCharsets.UTF_8), byteArray);
    }

    @Test
    void givenResultSetWithRows_whenGetAsciiStream_returnExpectedValue() throws Exception {
        final String cql = "SELECT col_ascii FROM tbl_test_texts WHERE keyname = 'key1'";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        final byte[] byteArray = IOUtils.toByteArray(rs.getAsciiStream("col_ascii"));
        assertArrayEquals("testValueAscii".getBytes(StandardCharsets.US_ASCII), byteArray);
    }

    @Test
    void givenVarintValue_whenFetching_returnExpectedValue() throws Exception {
        final String cql = "select (varint) 1 from system.local";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        Object result = rs.getObject(1);
        assertNotNull(result);
        assertEquals(BigInteger.valueOf(1), result);
    }

    @Test
    void givenResultSetWithRows_whenGetCharacterStream_returnExpectedValue() throws Exception {
        final String cql = "SELECT col_blob FROM tbl_test_blobs WHERE keyname = 'key1'";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        final byte[] byteArray = IOUtils.toByteArray(rs.getCharacterStream("col_blob"), StandardCharsets.UTF_8);
        assertArrayEquals("testValueAsClobInUtf8 with accents: Äîéè".getBytes(StandardCharsets.UTF_8), byteArray);
    }

    @Test
    void givenNullValue_whenFetchingValue_returnNull() throws Exception {
        final String cql = "select (int) null from system.local";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        Object result = rs.getObject(1);
        assertNull(result);
    }

    @Test
    void givenStringValue_whenFetchingAsByteBuffer_throwsCodecNotFoundException() throws Exception {
        final String cql = "SELECT keyname FROM cf_test1";
        final Statement statement = sqlConnection.createStatement();
        final ResultSet rs = statement.executeQuery(cql);
        final CodecNotFoundException exception = assertThrows(CodecNotFoundException.class,
            () -> rs.getBytes("keyname"));
        assertEquals("Codec not found for requested operation: [TEXT <-> java.nio.ByteBuffer]", exception.getMessage());
    }

    @Test
    void givenStringValue_whenFetchingAsByteBufferWithAppropriateCustomCodec_returnExpectedValue() {
        try (Connection connectionWithCustomCodec = newConnection(KEYSPACE, "localdatacenter=datacenter1",
            "customcodecs=com.ing.data.cassandra.jdbc.testing.TextToByteBufferCodec")) {
            final String cql = "SELECT keyname FROM cf_test1";
            final Statement statement = connectionWithCustomCodec.createStatement();
            final ResultSet rs = statement.executeQuery(cql);
            final byte[] result = rs.getBytes("keyname");
            assertNotNull(result);
            assertEquals("key1", new String(result));
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    void givenStatementFlaggedCloseOnCompletion_whenCloseResultSet_statementIsClosed() throws Exception {
        final String cql = "select (int) 1 from system.local";
        final Statement statement = sqlConnection.createStatement();
        statement.closeOnCompletion();
        final ResultSet rs = statement.executeQuery(cql);
        assertTrue(rs.next());
        assertFalse(statement.isClosed());
        rs.close();
        assertTrue(statement.isClosed());
    }

    static Stream<Arguments> buildMoveCursorNotForwardTestCases() {
        return Stream.of(
            Arguments.of("absolute", new Class<?>[]{ int.class }, new Object[]{ 0 }),
            Arguments.of("afterLast", null, null),
            Arguments.of("beforeFirst", null, null),
            Arguments.of("first", null, null),
            Arguments.of("last", null, null),
            Arguments.of("previous", null, null),
            Arguments.of("relative", new Class<?>[]{ int.class }, new Object[]{ 0 })
        );
    }

    @ParameterizedTest
    @MethodSource("buildMoveCursorNotForwardTestCases")
    void givenForwardOnlyStatement_whenMoveCursorNotForward_throwException(
        final String moveCursorMethodName, final Class<?>[] argsTypes, final Object[] args
    ) throws SQLException {
        final String cql = "SELECT keyname FROM cf_test1";
        final Statement statement = sqlConnection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        final ResultSet rs = statement.executeQuery(cql);
        final InvocationTargetException ex = assertThrows(InvocationTargetException.class,
            () -> ResultSet.class.getMethod(moveCursorMethodName, argsTypes).invoke(rs, args)
        );
        assertInstanceOf(SQLNonTransientException.class, ex.getCause());
        final SQLNonTransientException sqlEx = (SQLNonTransientException) ex.getCause();
        assertEquals(FORWARD_ONLY, sqlEx.getMessage());
        rs.close();
    }

    static Stream<Arguments> buildMoveCursorOnClosedResultSetTestCases() {
        return Stream.of(
            Arguments.of("absolute", new Class<?>[]{ int.class }, new Object[]{ 0 }),
            Arguments.of("afterLast", null, null),
            Arguments.of("beforeFirst", null, null),
            Arguments.of("first", null, null),
            Arguments.of("last", null, null),
            Arguments.of("next", null, null),
            Arguments.of("previous", null, null),
            Arguments.of("relative", new Class<?>[]{ int.class }, new Object[]{ 0 })
        );
    }

    @ParameterizedTest
    @MethodSource("buildMoveCursorOnClosedResultSetTestCases")
    void givenClosedResultSet_whenMoveCursor_throwException(
        final String moveCursorMethodName, final Class<?>[] argsTypes, final Object[] args
    ) throws SQLException {
        final String cql = "SELECT keyname FROM cf_test1";
        final Statement statement = sqlConnection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        final ResultSet rs = statement.executeQuery(cql);
        rs.close();
        final InvocationTargetException ex = assertThrows(InvocationTargetException.class,
            () -> ResultSet.class.getMethod(moveCursorMethodName, argsTypes).invoke(rs, args)
        );
        assertInstanceOf(SQLException.class, ex.getCause());
        final SQLException sqlEx = (SQLException) ex.getCause();
        assertEquals(WAS_CLOSED_RS, sqlEx.getMessage());
        rs.close();
    }

    @Test
    void givenScrollableStatement_whenMoveCursor_moveToExpectedRow() throws SQLException {
        final Statement insertStatement = sqlConnection.createStatement();
        for (int i = 0; i < 5; i++) {
            insertStatement.addBatch(
                "INSERT INTO cf_test1 (keyname, t1bValue, t1iValue) VALUES('key" + i + "', true, " + i + ")"
            );
        }
        insertStatement.executeBatch();
        insertStatement.close();

        final String cql = "SELECT keyname FROM cf_test1";
        final Statement selectStatement = sqlConnection.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY);
        final ResultSet rs = selectStatement.executeQuery(cql);
        // The result set for this query should contain 5 rows. At the beginning, the cursor is positioned before the
        // first row (row number = 0).
        // Move to next row, should return the first row of the result set (row number = 1).
        assertTrue(rs.next());
        assertEquals(1, rs.getRow());

        // Move before first row, using previous (current row - 1).
        assertFalse(rs.previous());
        assertTrue(rs.isBeforeFirst());

        // Move after last row.
        rs.afterLast();
        assertTrue(rs.isAfterLast());

        // Move forward 2 rows, should stay after the last row the result set.
        assertFalse(rs.relative(2));
        assertTrue(rs.isAfterLast());

        // Move before first row.
        rs.beforeFirst();
        assertTrue(rs.isBeforeFirst());

        // Move to last row (row number = 5).
        assertTrue(rs.last());
        assertEquals(5, rs.getRow());

        // Move to previous row (row number = 4).
        assertTrue(rs.previous());
        assertEquals(4, rs.getRow());

        // Move to first row (row number = 1).
        assertTrue(rs.first());
        assertEquals(1, rs.getRow());

        // Move forward 3 rows, should return the fourth row of the result set (current row + 3 = 4).
        assertTrue(rs.relative(3));
        assertEquals(4, rs.getRow());

        // Move backward 2 rows, should return the second row of the result set (current row - 2 = 2).
        assertTrue(rs.relative(-2));
        assertEquals(2, rs.getRow());

        // Keep the cursor at the same position, should stay on the second row of the result set (row number = 2).
        assertTrue(rs.relative(0));
        assertEquals(2, rs.getRow());

        // Move to the third row, using absolute cursor position (row number = 3).
        assertTrue(rs.absolute(3));
        assertEquals(3, rs.getRow());

        // Move to the second row, using absolute negative cursor position (row number = 2).
        assertTrue(rs.absolute(-4));
        assertEquals(2, rs.getRow());

        // Move before first row, using absolute value 0.
        assertFalse(rs.absolute(0));
        assertTrue(rs.isBeforeFirst());

        // Move backward 2 rows, should stay before the first row the result set.
        assertFalse(rs.relative(-2));
        assertTrue(rs.isBeforeFirst());

        rs.close();
        selectStatement.close();
    }

    @Test
    void givenScrollableStatementWithEmptyResultSet_whenMoveCursor_doNothing() throws SQLException {
        final String cql = "SELECT keyname FROM cf_test1 WHERE keyname = 'not found'";
        final Statement selectStatement = sqlConnection.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY);
        final ResultSet rs = selectStatement.executeQuery(cql);

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.next());
        assertFalse(rs.last());
        assertFalse(rs.previous());
        assertFalse(rs.first());
        assertFalse(rs.relative(2));
        assertTrue(rs.isAfterLast());
        assertFalse(rs.relative(-2));
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.absolute(2));
        assertTrue(rs.isAfterLast());
        assertFalse(rs.absolute(-2));
        assertTrue(rs.isBeforeFirst());

        rs.close();
        selectStatement.close();
    }

}
