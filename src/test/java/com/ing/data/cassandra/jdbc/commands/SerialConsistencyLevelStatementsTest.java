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
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static com.ing.data.cassandra.jdbc.utils.ConsistencyLevelTestUtils.assertSerialConsistencyLevel;
import static com.ing.data.cassandra.jdbc.utils.ConsistencyLevelTestUtils.assertSerialConsistencyLevelViaExecute;
import static java.sql.Statement.EXECUTE_FAILED;
import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Force to execute tests in a certain order to check the serial consistency level on connection as expected.
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SerialConsistencyLevelStatementsTest extends UsingCassandraContainerTest {
    private static final String KEYSPACE = "test_keyspace";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1", "serialconsistency=SERIAL");
    }

    @Test
    @Order(1)
    void givenDefaultSerialConsistencyLevel_whenGetSerialConsistencyLevel_returnExpectedSerialConsistencyLevel()
        throws Exception {
        assertNotNull(sqlConnection);
        assertSerialConsistencyLevel(sqlConnection, "SERIAL");
    }

    @Test
    @Order(2)
    void givenMultipleStatementsWithSerialConsistencyLevelChange_whenExecute_updateSerialConsistencyLevel()
        throws Exception {
        assertNotNull(sqlConnection);
        assertSerialConsistencyLevel(sqlConnection, "SERIAL");
        sqlConnection.createStatement().execute("SERIAL CONSISTENCY LOCAL_SERIAL;"
            + "INSERT INTO cf_test1 (keyname, t1bValue, t1iValue) VALUES('keyCL', true, 0) IF NOT EXISTS;");
        assertSerialConsistencyLevel(sqlConnection, "LOCAL_SERIAL");
    }

    @Test
    @Order(3)
    void givenSerialConsistencyLevel_whenSetSerialConsistencyLevel_updateSerialConsistencyLevel() throws Exception {
        assertNotNull(sqlConnection);
        assertSerialConsistencyLevel(sqlConnection, "LOCAL_SERIAL");
        Statement statement = sqlConnection.createStatement();
        boolean isQuery = statement.execute("SERIAL CONSISTENCY SERIAL");
        assertFalse(isQuery);
        assertSerialConsistencyLevel(sqlConnection, "SERIAL");
    }

    @Test
    @Order(4)
    void givenSerialConsistencyLevel_whenSetSerialConsistencyLevelViaExecute_updateSerialConsistencyLevel()
        throws Exception {
        assertNotNull(sqlConnection);
        sqlConnection.createStatement().execute("SERIAL CONSISTENCY LOCAL_SERIAL");
        assertSerialConsistencyLevelViaExecute(sqlConnection, "LOCAL_SERIAL");
    }

    @Test
    @Order(5)
    void givenInvalidSerialConsistencyLevel_whenSetSerialConsistencyLevel_throwException() {
        assertNotNull(sqlConnection);
        assertThrows(SQLException.class,
            () -> sqlConnection.createStatement().execute("SERIAL CONSISTENCY invalid_consistency"));
    }

    @Test
    @Order(6)
    void givenBatchIncludingSerialConsistencyLevelChanges_whenExecuteBatch_throwException() throws SQLException {
        // executeBatch() method currently does not support special commands.
        assertNotNull(sqlConnection);
        sqlConnection.createStatement().execute("SERIAL CONSISTENCY SERIAL");

        final Statement batchStatement = sqlConnection.createStatement();
        batchStatement.addBatch("SERIAL CONSISTENCY LOCAL_SERIAL");
        for (int i = 0; i < 5; i++) {
            batchStatement.addBatch(
                "INSERT INTO cf_test1 (keyname, t1bValue, t1iValue) VALUES('key" + i + "', true, " + i + ")"
            );
        }

        final BatchUpdateException ex = assertThrows(BatchUpdateException.class, batchStatement::executeBatch);
        assertThat(ex.getMessage(),
            matchesPattern("^At least one statement in batch has failed:\n - Statement #0: .*$")
        );
        final int[] counts = ex.getUpdateCounts();
        assertEquals(6, counts.length);
        assertEquals(EXECUTE_FAILED, counts[0]);
        assertTrue(Arrays.stream(ArrayUtils.subarray(counts, 1, 5)).allMatch(c -> c == SUCCESS_NO_INFO));

        batchStatement.close();
    }

    @Test
    @Order(7)
    void givenConsistencyLevel_whenSetConsistencyLevelViaLowercaseCommand_updateConsistencyLevel() throws Exception {
        assertNotNull(sqlConnection);
        sqlConnection.createStatement().execute("serial consistency LOCAL_SERIAL");
        assertSerialConsistencyLevelViaExecute(sqlConnection, "LOCAL_SERIAL");
    }
}
