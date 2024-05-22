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
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Calendar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    void givenIncompleteResultSet_whenFindColumns_throwException() {
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
        when(mockDriverRs.getExecutionInfo()).thenReturn(mock(ExecutionInfo.class));
        when(mockDriverRs.getExecutionInfo().getWarnings())
            .thenReturn(Arrays.asList("First warning message", "Second warning message"));
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
}
