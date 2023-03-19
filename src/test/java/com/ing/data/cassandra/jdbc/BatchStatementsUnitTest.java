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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Force to execute tests in a certain order to avoid "NoNodeAvailableException: No node was available to execute the
// query" if several tests are executed simultaneously.
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BatchStatementsUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace_batch";
    private static CassandraConnection sqlConnection2 = null;

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "version=3.0.0", "localdatacenter=datacenter1");
        sqlConnection2 = newConnection(KEYSPACE, "version=3.0.0", "localdatacenter=datacenter1");
    }

    @AfterAll
    static void afterTests() throws Exception {
        if (sqlConnection != null) {
            sqlConnection.close();
        }
        if (sqlConnection2 != null) {
            sqlConnection2.close();
        }
        cassandraContainer.stop();
    }

    @Test
    @Order(1)
    void givenBatchSimpleStatement_whenExecute_returnExpectedResult() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        stmt.execute("TRUNCATE collections_test");
        final Statement stmt2 = sqlConnection.createStatement();
        final int nbRows = CassandraStatement.MAX_ASYNC_QUERIES;

        for (int i = 0; i < nbRows; i++) {
            stmt2.addBatch("INSERT INTO collections_test (keyValue, listValue) VALUES( " + i + ", [1, 3, 12345])");
        }

        final int[] counts = stmt2.executeBatch();
        assertEquals(nbRows, counts.length);

        final StringBuilder queries = new StringBuilder();
        for (int i = 0; i < nbRows + 10; i++) {
            queries.append("SELECT * FROM collections_test WHERE keyValue = ").append(i).append(";");
        }
        final ResultSet result = stmt2.executeQuery(queries.toString());

        int nbRowsInResult = 0;
        final ArrayList<Integer> foundKeyValues = new ArrayList<>();
        while (result.next()) {
            nbRowsInResult++;
            foundKeyValues.add(result.getInt("keyValue"));
        }

        assertEquals(nbRows, nbRowsInResult);
        for (int i = 0; i < nbRows; i++) {
            assertTrue(foundKeyValues.contains(i));
        }

        stmt2.close();
    }

    @Test
    @Order(2)
    void givenBatchSimpleSplitStatement_whenExecute_returnExpectedResult() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        stmt.execute("TRUNCATE collections_test");
        final Statement stmt2 = sqlConnection.createStatement();
        final int nbRows = CassandraStatement.MAX_ASYNC_QUERIES;

        final StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < nbRows; i++) {
            queryBuilder.append("INSERT INTO collections_test (keyValue, listValue) VALUES( ").append(i)
                .append(",[1, 3, 12345]);");
        }
        stmt2.execute(queryBuilder.toString());

        final StringBuilder queries = new StringBuilder();
        for (int i = 0; i < nbRows; i++) {
            queries.append("SELECT * FROM collections_test WHERE keyValue = ").append(i).append(";");
        }
        final ResultSet result = stmt2.executeQuery(queries.toString());

        int nbRowsInResult = 0;
        final ArrayList<Integer> foundKeyValues = new ArrayList<>();
        while (result.next()) {
            nbRowsInResult++;
            foundKeyValues.add(result.getInt("keyValue"));
        }

        assertEquals(nbRows, nbRowsInResult);
        for (int i = 0; i < nbRows; i++) {
            assertTrue(foundKeyValues.contains(i));
        }

        stmt2.close();
    }

    @Test
    @Order(3)
    void givenBatchPreparedStatement_whenExecute_returnExpectedResult() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        stmt.execute("TRUNCATE collections_test");
        final PreparedStatement stmt2 = sqlConnection.prepareStatement("INSERT INTO collections_test " +
            "(keyValue, listValue) VALUES(?, ?)");
        final int nbRows = CassandraStatement.MAX_ASYNC_QUERIES;

        for (int i = 0; i < nbRows; i++) {
            stmt2.setInt(1, i);
            stmt2.setObject(2, Arrays.asList(1L, 3L, 12345L));
            stmt2.addBatch();
        }

        final int[] counts = stmt2.executeBatch();
        assertEquals(nbRows, counts.length);

        final StringBuilder queries = new StringBuilder();
        for (int i = 0; i < nbRows; i++) {
            queries.append("SELECT * FROM collections_test WHERE keyValue = ").append(i).append(";");
        }
        final ResultSet result = stmt2.executeQuery(queries.toString());

        int nbRowsInResult = 0;
        final ArrayList<Integer> foundKeyValues = new ArrayList<>();
        while (result.next()) {
            nbRowsInResult++;
            foundKeyValues.add(result.getInt("keyValue"));
        }

        assertEquals(nbRows, nbRowsInResult);
        for (int i = 0; i < nbRows; i++) {
            assertTrue(foundKeyValues.contains(i));
        }

        stmt2.close();
    }

    @Test
    @Order(4)
    void givenBatchPreparedStatementWithUnsetParameter_whenExecute_returnExpectedResult() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        stmt.execute("TRUNCATE collections_test");
        final PreparedStatement stmt2 = sqlConnection.prepareStatement("INSERT INTO collections_test " +
            "(keyValue, listValue, mapValue) VALUES(?, ?, ?)");
        final int nbRows = CassandraStatement.MAX_ASYNC_QUERIES;

        for (int i = 0; i < nbRows; i++) {
            stmt2.setInt(1, i);
            stmt2.setObject(2, Arrays.asList(1L, 3L, 12345L));
            stmt2.addBatch();
        }

        final int[] counts = stmt2.executeBatch();
        assertEquals(nbRows, counts.length);
    }

    @ParameterizedTest
    @Order(5)
    @ValueSource(ints = {1, 10})
    void givenAsyncSelectStatement_whenExecute_returnExpectedResult(final int nbRows) throws Exception {
        final PreparedStatement statement = sqlConnection2.prepareStatement("INSERT INTO collections_test " +
            "(keyValue, listValue) VALUES(?, ?)");

        for (int i = 0; i < nbRows; i++) {
            statement.setInt(1, i);
            statement.setObject(2, Arrays.asList(1L, 3L, 12345L));
            statement.addBatch();
        }

        final int[] counts = statement.executeBatch();
        assertEquals(nbRows, counts.length);

        final StringBuilder query = new StringBuilder();
        for (int i = 0; i < nbRows; i++) {
            query.append("SELECT * FROM collections_test WHERE keyValue = ").append(i).append(";");
        }

        final Statement selectStatement = sqlConnection2.createStatement();
        final ResultSet result = selectStatement.executeQuery(query.toString());

        int nbRowsInResult = 0;
        final ArrayList<Integer> foundKeyValues = new ArrayList<>();
        while (result.next()) {
            nbRowsInResult++;
            foundKeyValues.add(result.getInt("keyValue"));
        }

        assertEquals(nbRows, nbRowsInResult);
        for (int i = 0; i < nbRows; i++) {
            assertTrue(foundKeyValues.contains(i));
        }

        statement.close();
    }

    @Test
    @Order(6)
    void givenBatchSimpleSplitStatementWithErrors_whenExecute_throwException() throws Exception {
        final Statement stmt = sqlConnection.createStatement();
        stmt.execute("TRUNCATE collections_test");
        final Statement stmt2 = sqlConnection.createStatement();
        final int nbRows = CassandraStatement.MAX_ASYNC_QUERIES;

        final StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < nbRows; i++) {
            if (i % 100 == 0) {
                queryBuilder.append("INSERT INTO collections_test (keyValue, listValue, mapValue) VALUES( ").append(i)
                    .append(", [1, 3, 12345], 0);");
            } else {
                queryBuilder.append("INSERT INTO collections_test (keyValue, listValue) VALUES( ").append(i)
                    .append(", [1, 3, 12345]);");
            }
        }
        assertThrows(SQLTransientException.class, () -> stmt2.execute(queryBuilder.toString()));
    }
}
