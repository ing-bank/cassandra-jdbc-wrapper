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

import com.ing.data.cassandra.jdbc.utils.ContactPoint;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PooledUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace_pool";
    private static final String USER = "testuser";
    private static final String PASSWORD = "secret";
    private static final String CONSISTENCY = "ONE";
    private static final String LOCAL_DATACENTER = "datacenter1";

    @Test
    void givenPooledDataSource_whenGetAndCloseConnection2MillionTimes_manageConnectionsProperly() throws Exception {
        final CassandraDataSource connectionPoolDataSource = buildConnectionPoolDataSource();
        final DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

        for (int i = 0; i < 2_000_000; i++) {
            final Connection connection = pooledCassandraDataSource.getConnection();
            assertFalse(connection.isClosed());
            connection.close();
            assertTrue(connection.isClosed());
        }
    }

    @Test
    void givenPooledDataSource_whenExecute5ThousandsPreparedStatements_getExpectedResults() throws Exception {
        final CassandraDataSource connectionPoolDataSource = buildConnectionPoolDataSource();
        final DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);
        final Connection connection = pooledCassandraDataSource.getConnection();

        for (int i = 0; i < 5_000; i++) {
            final PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT someInt FROM pooled_test WHERE somekey = ?");
            preparedStatement.setString(1, "world");
            assertFalse(preparedStatement.isClosed());
            final ResultSet resultSet = preparedStatement.executeQuery();
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt("someInt"));
            assertFalse(resultSet.next());
            preparedStatement.close();
            assertTrue(preparedStatement.isClosed());
        }

        connection.close();
    }

    @Test
    void givenPooledDataSource_whenExecuteStatement_getExpectedResults() throws Exception {
        final CassandraDataSource connectionPoolDataSource = buildConnectionPoolDataSource();
        final DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);
        final Connection connection = pooledCassandraDataSource.getConnection();
        final Statement statement = connection.createStatement();
        assertFalse(statement.isClosed());
        final ResultSet resultSet = statement.executeQuery("SELECT someInt FROM pooled_test WHERE somekey = 'world'");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt("someInt"));
        assertFalse(resultSet.next());
        statement.close();
        connection.close();
        assertTrue(statement.isClosed());
    }

    @Test
    void givenPooledCassandraDataSource_whenUnwrap_returnUnwrappedDataSource() throws Exception {
        final CassandraDataSource connectionPoolDataSource = buildConnectionPoolDataSource();
        assertNotNull(connectionPoolDataSource.unwrap(DataSource.class));
    }

    @Test
    void givenPooledCassandraDataSource_whenUnwrapToInvalidInterface_throwException() {
        final CassandraDataSource connectionPoolDataSource = buildConnectionPoolDataSource();
        assertThrows(SQLException.class, () -> connectionPoolDataSource.unwrap(this.getClass()));
    }

    private static CassandraDataSource buildConnectionPoolDataSource() {
        final CassandraDataSource connectionPoolDataSource = new CassandraDataSource(
            Collections.singletonList(ContactPoint.of(
                cassandraContainer.getContactPoint().getHostName(), cassandraContainer.getContactPoint().getPort())),
            KEYSPACE);
        connectionPoolDataSource.setUser(USER);
        connectionPoolDataSource.setPassword(PASSWORD);
        connectionPoolDataSource.setConsistency(CONSISTENCY);
        connectionPoolDataSource.setLocalDataCenter(LOCAL_DATACENTER);
        return connectionPoolDataSource;
    }
}
