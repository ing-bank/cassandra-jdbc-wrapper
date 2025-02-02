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

import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CopyCommandsTestUtils {

    public static final String COPY_CMD_TEST_KEYSPACE = "copy_cmd_keyspace";

    public static final String COPY_CMD_TEST_TABLE_NAME = "copy_cmd_table";
    public static final Pair<String, String> COPY_CMD_TEST_TABLE =
        Pair.of(COPY_CMD_TEST_TABLE_NAME, "table_key, bool_val, decimal_val");

    public static final String COPY_CMD_TEST_PARTIAL_TABLE_NAME = "copy_cmd_skip_rows_table";
    public static final Pair<String, String> COPY_CMD_TEST_PARTIAL_TABLE =
        Pair.of(COPY_CMD_TEST_PARTIAL_TABLE_NAME, "table_key, int_val, str_val");

    public static final String COPY_CMD_TEST_ALL_TYPES_TABLE = "copy_cmd_all_types_table";

    public static void assertRowValues(final Connection connection,
                                       final Pair<String, String> tableDesc,
                                       final Object... expectedValues) throws SQLException {
        assert expectedValues != null && expectedValues.length > 0 : "Specify at least one expected value";
        final PreparedStatement verifyStmt = connection.prepareStatement(
            String.format("SELECT %s FROM %s WHERE table_key = ?", tableDesc.getValue(), tableDesc.getKey()));
        verifyStmt.setObject(1, expectedValues[0]);
        final ResultSet verifyRs = verifyStmt.executeQuery();
        assertTrue(verifyRs.next());
        for (int i = 1; i <= expectedValues.length; i++) {
            assertEquals(expectedValues[i - 1], verifyRs.getObject(i));
        }
        verifyRs.close();
        verifyStmt.close();
    }

}
