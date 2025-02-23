/*
 *
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

package com.ing.data.cassandra.jdbc.commands;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.ing.data.cassandra.jdbc.UsingCassandraContainerTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_ALL_TYPES_TABLE;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_ALL_TYPES_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_KEYSPACE;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_PARTIAL_TABLE;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_PARTIAL_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_TABLE;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.assertCommandResultSet;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.assertRowValues;
import static com.ing.data.cassandra.jdbc.utils.ByteBufferUtil.bytes;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class CopyFromCommandTest extends UsingCassandraContainerTest {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFromCommandTest.class);

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(COPY_CMD_TEST_KEYSPACE, "localdatacenter=datacenter1");
    }

    static String getTestOriginPath(final String csvFile, final boolean allowInvalidFile) {
        final URL cqlScriptResourceUrl =
            CopyFromCommandTest.class.getClassLoader().getResource("copyFromTests/" + csvFile);
        if (cqlScriptResourceUrl == null) {
            if (allowInvalidFile) {
                return csvFile;
            }
            fail("Could not find the CSV script to import in 'copyFromTests' directory: " + csvFile);
        }
        return cqlScriptResourceUrl.getPath();
    }

    private ResultSet executeCopyFromCommand(final String targetTable, final String csvSourceFile,
                                             final String options) throws SQLException {
        return executeCopyFromCommand(targetTable, csvSourceFile, options, false);
    }

    private ResultSet executeCopyFromCommand(final String targetTable, final String csvSourceFile,
                                             final String options, final boolean allowInvalidFile) throws SQLException {
        assertNotNull(sqlConnection);
        // First, truncate target table to ensure the data of any test previously executed is removed.
        final Statement truncateTableStmt = sqlConnection.createStatement();
        truncateTableStmt.execute(String.format("TRUNCATE TABLE %s", targetTable));
        truncateTableStmt.close();

        final Statement copyCmdStmt = sqlConnection.createStatement();
        copyCmdStmt.execute(String.format("COPY %s FROM '%s' %s", targetTable,
            getTestOriginPath(csvSourceFile, allowInvalidFile), Optional.ofNullable(options).orElse(EMPTY)));
        return copyCmdStmt.getResultSet();
    }

    static Stream<Arguments> buildCopyFromCommandVariableParameters() {
        return Stream.of(
            Arguments.of("test_simple.csv", EMPTY),
            Arguments.of("test_with_headers.csv", "WITH HEADER=true"),
            Arguments.of("test_with_special_format.csv",
                "WITH DELIMITER=| AND QUOTE=` AND DECIMALSEP=, AND THOUSANDSSEP=.")
        );
    }

    @ParameterizedTest
    @MethodSource("buildCopyFromCommandVariableParameters")
    void givenTableAndOriginFile_whenExecuteCopyFromCommand_executeExpectedStatements(final String csvFile,
                                                                                      final String options)
        throws SQLException {
        final ResultSet resultSet = executeCopyFromCommand(COPY_CMD_TEST_TABLE_NAME, csvFile, options);
        assertCommandResultSet(resultSet, true, 2, 1, 0);
        assertRowValues(sqlConnection, COPY_CMD_TEST_TABLE, "key1", true, new BigDecimal("654000.7"));
        assertRowValues(sqlConnection, COPY_CMD_TEST_TABLE, "key2", false, new BigDecimal("321000.8"));
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithSkipRows_executeExpectedStatements()
        throws SQLException {
        final ResultSet resultSet = executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME,
            "test_partial_import.csv", "WITH SKIPROWS=2");
        assertCommandResultSet(resultSet, true, 2, 1, 2);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, "N/A");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key4", 4, "test4");
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithMaxRows_executeExpectedStatements()
        throws SQLException {
        final ResultSet resultSet = executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME,
            "test_partial_import.csv", "WITH MAXROWS=1");
        assertCommandResultSet(resultSet, true, 1, 1, 3);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key1", 1, "test1");
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithPartialImportOptions_executeExpectedStatements()
        throws SQLException {
        final ResultSet resultSet = executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME, "test_partial_import.csv",
            "WITH MAXROWS=2 AND SKIPROWS=1 AND SKIPCOLS=str_val");
        assertCommandResultSet(resultSet, true, 2, 1, 2);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key2", 2, null);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, null);
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithNullValueOption_executeExpectedStatements()
        throws SQLException {
        final ResultSet resultSet = executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME,
            "test_partial_import.csv", "WITH NULLVAL=N/A");
        assertCommandResultSet(resultSet, true, 4, 1, 0);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key1", 1, "test1");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, null);
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithUnsupportedOptions_throwException() {
        final SQLException sqlException = assertThrows(SQLException.class, () ->
            executeCopyFromCommand(COPY_CMD_TEST_TABLE_NAME, "test_simple.csv", "WITH BADOPTION=1"));
        assertThat(sqlException.getMessage(),
            containsString("Command COPY used with unknown or unsupported options: [BADOPTION]"));
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithInvalidIntOption_useDefaultOptionValue()
        throws SQLException {
        final ResultSet resultSet = executeCopyFromCommand(
            COPY_CMD_TEST_KEYSPACE + "." + COPY_CMD_TEST_PARTIAL_TABLE_NAME, "test_partial_import.csv",
            "WITH SKIPROWS=a");
        assertCommandResultSet(resultSet, true, 4, 1, 0);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key1", 1, "test1");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key2", 2, "test2");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, "N/A");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key4", 4, "test4");
    }

    @Test
    void givenNonExistingFile_whenExecuteCopyFromCommand_throwException() {
        final SQLException sqlException = assertThrows(SQLException.class, () ->
            executeCopyFromCommand(COPY_CMD_TEST_TABLE_NAME, "bad_test_file.csv", EMPTY, true));
        assertThat(sqlException.getMessage(),
            containsString("Could not find CSV file to import 'bad_test_file.csv'"));
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithSpecificBatchSize_executeExpectedBatches()
        throws SQLException {
        final ResultSet resultSet = executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME,
            "test_partial_import.csv", "WITH BATCHSIZE=2");
        assertCommandResultSet(resultSet, true, 4, 2, 0);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key1", 1, "test1");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key2", 2, "test2");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, "N/A");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key4", 4, "test4");
    }

    @Test
    void givenTableWithAllTypesAndOriginFile_whenExecuteCopyFromCommand_executeExpectedStatements() throws Exception {
        final ResultSet resultSet = executeCopyFromCommand(COPY_CMD_TEST_ALL_TYPES_TABLE_NAME,
            "test_all_types_import.csv", "WITH HEADER=true AND DELIMITER=;");
        assertCommandResultSet(resultSet, true, 1, 1, 0);

        assertRowValues(sqlConnection, COPY_CMD_TEST_ALL_TYPES_TABLE,
            "key1",                                                     // text
            "abc123",                                                   // ascii
            12345678900000L,                                            // bigint
            bytes("this is a blob"),                                    // blob
            true,                                                       // boolean
            Date.valueOf("2023-03-25"),                                 // date
            new BigDecimal("18.97"),                                    // decimal
            2345.6d,                                                    // double
            CqlDuration.from("3h15m"),                                  // duration
            21.3f,                                                      // float
            Inet4Address.getByName("127.0.0.1"),                        // inet
            98,                                                         // int
            Arrays.asList(4, 6, 10),                                    // list
            new HashMap<Integer, String>(){{                            // map
                put(3, "three");
                put(8, "eight");
            }},
            (short) 2,                                                  // smallint
            new HashSet<Integer>(){{                                    // set
                add(2);
                add(3);
                add(5);
            }},
            Time.valueOf("12:30:45"),                                   // time
            Timestamp.valueOf(                                          // timestamp
                OffsetDateTime.parse("2023-03-25T12:30:45.789Z")
                    .atZoneSameInstant(ZoneId.systemDefault())
                    .toLocalDateTime()),
            UUID.fromString("1f4b6fe0-f12b-11ef-8b6b-fdf7025e383c"),    // timeuuid
            (byte) 12,                                                  // tinyint
            new DefaultTupleValue(                                      // tuple
                new DefaultTupleType(Arrays.asList(DataTypes.INT, DataTypes.TEXT)),
                5, "five"),
            UUID.fromString("f26f86e6-d8ef-4d30-9d9e-4027d21e02a9"),    // uuid
            "varchar text",                                             // varchar
            new BigInteger("4321"),                                     // varint
            CqlVector.newInstance(1, 3, 8, 6)                           // vector
            );
    }

}
