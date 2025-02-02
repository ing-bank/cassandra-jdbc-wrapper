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

import com.ing.data.cassandra.jdbc.UsingCassandraContainerTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.utils.CopyCommandsTestUtils.COPY_CMD_TEST_KEYSPACE;
import static com.ing.data.cassandra.jdbc.utils.CopyCommandsTestUtils.COPY_CMD_TEST_PARTIAL_TABLE;
import static com.ing.data.cassandra.jdbc.utils.CopyCommandsTestUtils.COPY_CMD_TEST_PARTIAL_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.utils.CopyCommandsTestUtils.COPY_CMD_TEST_TABLE;
import static com.ing.data.cassandra.jdbc.utils.CopyCommandsTestUtils.COPY_CMD_TEST_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.utils.CopyCommandsTestUtils.assertRowValues;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class CopyFromCommandTest extends UsingCassandraContainerTest {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFromCommandTest.class);

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(COPY_CMD_TEST_KEYSPACE, "localdatacenter=datacenter1");
    }

    static String getTestOriginPath(final String csvFile) {
        final URL cqlScriptResourceUrl =
            CopyFromCommandTest.class.getClassLoader().getResource("copyFromTests/" + csvFile);
        if (cqlScriptResourceUrl == null) {
            fail("Could not find the CSV script to import in 'copyFromTests' directory: " + csvFile);
        }
        return cqlScriptResourceUrl.getPath();
    }

    private void executeCopyFromCommand(final String targetTable, final String csvSourceFile,
                                        final String options) throws SQLException {
        assertNotNull(sqlConnection);
        final Statement truncateTableStmt = sqlConnection.createStatement();
        truncateTableStmt.execute(String.format("TRUNCATE TABLE %s", targetTable));
        truncateTableStmt.close();
        sqlConnection.createStatement().execute(String.format("COPY %s FROM '%s' %s", targetTable,
                getTestOriginPath(csvSourceFile), options));
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
        executeCopyFromCommand(COPY_CMD_TEST_TABLE_NAME, csvFile, options);
        assertRowValues(sqlConnection, COPY_CMD_TEST_TABLE, "key1", true, new BigDecimal("654000.7"));
        assertRowValues(sqlConnection, COPY_CMD_TEST_TABLE, "key2", false, new BigDecimal("321000.8"));
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithSkipRows_executeExpectedStatements()
        throws SQLException {
        executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME, "test_partial_import.csv", "WITH SKIPROWS=2");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, "N/A");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key4", 4, "test4");
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithMaxRows_executeExpectedStatements()
        throws SQLException {
        executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME, "test_partial_import.csv", "WITH MAXROWS=1");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key1", 1, "test1");
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithPartialImportOptions_executeExpectedStatements()
        throws SQLException {
        executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME, "test_partial_import.csv",
            "WITH MAXROWS=2 AND SKIPROWS=1 AND SKIPCOLS=str_val");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key2", 2, null);
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, null);
    }

    @Test
    void givenTableAndOriginFile_whenExecuteCopyFromCommandWithNullValueOption_executeExpectedStatements()
        throws SQLException {
        executeCopyFromCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME, "test_partial_import.csv", "WITH NULLVAL=N/A");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key1", 1, "test1");
        assertRowValues(sqlConnection, COPY_CMD_TEST_PARTIAL_TABLE, "key3", 3, null);
    }

}
