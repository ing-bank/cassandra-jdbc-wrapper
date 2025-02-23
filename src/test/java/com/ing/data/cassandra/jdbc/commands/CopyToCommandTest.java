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
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ing.data.cassandra.jdbc.UsingCassandraContainerTest;
import org.apache.commons.collections4.CollectionUtils;
import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_ALL_TYPES_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_KEYSPACE;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_PARTIAL_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.COPY_CMD_TEST_TABLE_NAME;
import static com.ing.data.cassandra.jdbc.testing.CopyCommandsTestUtils.assertCommandResultSet;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CopyToCommandTest extends UsingCassandraContainerTest {

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(COPY_CMD_TEST_KEYSPACE, "localdatacenter=datacenter1");

        // Populate tables with data to export.
        sqlConnection.createStatement().execute(QueryBuilder.insertInto(COPY_CMD_TEST_TABLE_NAME)
                .value("table_key", literal("key1"))
                .value("bool_val", literal(true))
                .value("decimal_val", literal(1.5))
            .asCql()
        );
        sqlConnection.createStatement().execute(QueryBuilder.insertInto(COPY_CMD_TEST_TABLE_NAME)
            .value("table_key", literal("key2"))
            .value("bool_val", literal(false))
            .value("decimal_val", literal(4200))
            .asCql()
        );
        sqlConnection.createStatement().execute(QueryBuilder.insertInto(COPY_CMD_TEST_TABLE_NAME)
            .value("table_key", literal("key3"))
            .value("bool_val", literal(false))
            .value("decimal_val", literal(null))
            .asCql()
        );

        sqlConnection.createStatement().execute(QueryBuilder.insertInto(COPY_CMD_TEST_PARTIAL_TABLE_NAME)
            .value("table_key", literal("key1"))
            .value("int_val", literal(42))
            .value("str_val", literal("A string containing a character \" to escape"))
            .asCql()
        );

        sqlConnection.createStatement().execute(QueryBuilder.insertInto(COPY_CMD_TEST_ALL_TYPES_TABLE_NAME)
            .value("table_key", literal("key1"))
            .value("ascii_val", literal("abc123"))
            .value("bigint_val", literal(12345678900000L))
            .value("blob_val", function("textAsBlob", literal("this is a blob")))
            .value("bool_val", literal(true))
            .value("date_val", literal("2023-03-25"))
            .value("decimal_val", literal(new BigDecimal("18.97")))
            .value("double_val", literal(2345.6d))
            .value("duration_val", literal(CqlDuration.from("3h15m")))
            .value("float_val", literal(21.3f))
            .value("inet_val", literal("127.0.0.1"))
            .value("int_val", literal(98))
            .value("list_val", literal(Arrays.asList(4, 6, 10)))
            .value("map_val", literal(new HashMap<Integer, String>(){{
                put(3, "three");
                put(8, "eight");
            }}))
            .value("smallint_val", literal(2))
            .value("set_val", literal(new HashSet<Integer>(){{
                add(2);
                add(3);
                add(5);
            }}))
            .value("time_val", literal("12:30:45.678"))
            .value("ts_val", literal("2023-03-25T12:30:45.789"))
            .value("timeuuid_val", literal(UUID.fromString("1f4b6fe0-f12b-11ef-8b6b-fdf7025e383c")))
            .value("tinyint_val", literal(12))
            .value("tuple_val", tuple(literal(5), literal("five")))
            .value("uuid_val", literal(UUID.fromString("f26f86e6-d8ef-4d30-9d9e-4027d21e02a9")))
            .value("varchar_val", literal("varchar text"))
            .value("varint_val", literal(new BigInteger("4321")))
            .value("vector_val", literal(CqlVector.newInstance(1, 3, 8, 6)))
            .asCql()
        );
    }

    private ResultSet executeCopyToCommand(final String sourceTable, final List<String> sourceColumns,
                                           final String targetCsvFile, final String options) throws SQLException {
        assertNotNull(sqlConnection);
        final Statement copyCmdStmt = sqlConnection.createStatement();

        String formattedSourceColumns = EMPTY;
        if (CollectionUtils.isNotEmpty(sourceColumns)) {
            formattedSourceColumns = String.format("(%s)", String.join(",", sourceColumns));
        }

        copyCmdStmt.execute(String.format("COPY %s%s TO '%s' %s", sourceTable, formattedSourceColumns,
            targetCsvFile, Optional.ofNullable(options).orElse(EMPTY)));
        return copyCmdStmt.getResultSet();
    }

    private static Options approvalTestsParameterName(final String csvFile) {
        return Approvals.NAMES.withParameters(csvFile.replace(".csv", EMPTY));
    }

    static Stream<Arguments> buildCopyToCommandVariableParameters() {
        return Stream.of(
            Arguments.of("test_default_options.csv", EMPTY),
            Arguments.of("test_with_header.csv", "WITH HEADER=true"),
            Arguments.of("test_with_format_options.csv",
                "WITH DELIMITER=| AND QUOTE=` AND DECIMALSEP=, AND THOUSANDSSEP=. AND NULLVAL=N/A")
        );
    }

    @ParameterizedTest
    @MethodSource("buildCopyToCommandVariableParameters")
    void givenTableAndTargetFile_whenExecuteCopyToCommand_generateExpectedCsvFile(final String csvFile,
                                                                                  final String options)
        throws SQLException {
        final ResultSet resultSet = executeCopyToCommand(COPY_CMD_TEST_TABLE_NAME, null, csvFile, options);
        assertCommandResultSet(resultSet, false, 3, 1);
        Approvals.verify(new File(csvFile), approvalTestsParameterName(csvFile));
    }

    @Test
    void givenTableAndTargetFile_whenExecuteCopyToCommandWithEscapedChars_generateExpectedCsvFile()
        throws SQLException {
        final String targetCsvFile = "test_with_escape.csv";
        final ResultSet resultSet = executeCopyToCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME, null, targetCsvFile,
            "WITH ESCAPE=\"");
        assertCommandResultSet(resultSet, false, 1, 1);
        Approvals.verify(new File(targetCsvFile), approvalTestsParameterName(targetCsvFile));
    }

    @Test
    void givenTableWithColumnsAndTargetFile_whenExecuteCopyToCommand_generateExpectedCsvFile() throws SQLException {
        final String targetCsvFile = "test_partial_export.csv";
        final ResultSet resultSet = executeCopyToCommand(COPY_CMD_TEST_PARTIAL_TABLE_NAME,
            Arrays.asList("table_key", "int_val"), targetCsvFile, EMPTY);
        assertCommandResultSet(resultSet, false, 1, 1);
        Approvals.verify(new File(targetCsvFile), approvalTestsParameterName(targetCsvFile));
    }

    @Test
    void givenTableAndTargetFile_whenExecuteCopyToCommandUnsupportedOptions_throwException() throws SQLException {
        final SQLException sqlException = assertThrows(SQLException.class, () ->
            executeCopyToCommand(COPY_CMD_TEST_TABLE_NAME, null, "test_exception.csv", "WITH BADOPTION=1"));
        assertThat(sqlException.getMessage(),
            containsString("Command COPY used with unknown or unsupported options: [BADOPTION]"));
    }

    @Test
    void givenTableWithAllTypesAndTargetFile_whenExecuteCopyFromCommand_executeExpectedStatements() throws Exception {
        final String targetCsvFile = "test_all_types_export.csv";
        final ResultSet resultSet = executeCopyToCommand(COPY_CMD_TEST_ALL_TYPES_TABLE_NAME, null, targetCsvFile,
            "WITH HEADER=true AND DELIMITER=;");
        assertCommandResultSet(resultSet, false, 1, 1);
        Approvals.verify(new File(targetCsvFile), approvalTestsParameterName(targetCsvFile));
    }

}
