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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ing.data.cassandra.jdbc.testing.AssertionsUtils.assertConnectionHasExpectedConfig;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PooledDataSourceUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace_pool";
    private static final String USER = "testuser";
    private static final String PASSWORD = "secret";
    private static final String CONSISTENCY = "ONE";
    private static final String LOCAL_DATACENTER = "datacenter1";
    private static final List<ContactPoint> CONTACT_POINTS = Collections.singletonList(
        ContactPoint.of(cassandraContainer.getContactPoint().getHostName(),
            cassandraContainer.getContactPoint().getPort())
    );

    private static final int POOL_MIN_IDLE = 2;
    private static final int POOL_MAX_SIZE = 20;
    private static final int POOL_CONNECTION_TIMEOUT = 5_000;
    private static final int POOL_IDLE_TIMEOUT = 10_000;

    private PooledCassandraDataSource sut;
    private HikariDataSource hikariDS;

    private static CassandraDataSource buildConnectionPoolDataSource() {
        final CassandraDataSource connectionPoolDataSource = new CassandraDataSource(CONTACT_POINTS, KEYSPACE);
        connectionPoolDataSource.setUser(USER);
        connectionPoolDataSource.setPassword(PASSWORD);
        connectionPoolDataSource.setConsistency(CONSISTENCY);
        connectionPoolDataSource.setLocalDataCenter(LOCAL_DATACENTER);
        return connectionPoolDataSource;
    }

    @BeforeEach
    void setUp() {
        this.sut = new PooledCassandraDataSource(buildConnectionPoolDataSource());

        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSourceClassName(null);
        hikariConfig.setDataSource(buildDataSourceForHikari());
        hikariConfig.setMaximumPoolSize(POOL_MAX_SIZE);
        hikariConfig.setMinimumIdle(POOL_MIN_IDLE);
        hikariConfig.setConnectionTimeout(POOL_CONNECTION_TIMEOUT);
        hikariConfig.setIdleTimeout(POOL_IDLE_TIMEOUT);

        this.hikariDS = new HikariDataSource(hikariConfig);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (this.hikariDS != null && !hikariDS.isClosed()) {
            this.hikariDS.close();
        }

        try (
            final Connection conn = buildConnectionPoolDataSource().getConnection();
            final Statement stmt = conn.createStatement()
        ) {
            stmt.execute("TRUNCATE TABLE pooled_test");
        }
    }

    /**
     * HikariCP can accept a {@link ConnectionPoolDataSource} directly via {@code setDataSource()} when it also
     * implements {@link DataSource}. Even it's the case for {@link PooledCassandraDataSource}, the implementation of
     * {@code getConnection()} methods is not appropriate for testing via a pool manager, so, simply wrap the access to
     * the underlying connection from the pooled connection.
     */
    private DataSource buildDataSourceForHikari() {
        return new DataSource() {
            @Override public Connection getConnection() throws SQLException {
                return sut.getPooledConnection().getConnection();
            }

            @Override public Connection getConnection(final String user, final String password) throws SQLException {
                return sut.getPooledConnection(user, password).getConnection();
            }

            @Override public java.io.PrintWriter getLogWriter() {
                return null;
            }

            @Override public void setLogWriter(final java.io.PrintWriter pw) {
            }

            @Override public void setLoginTimeout(final int timeout) {
            }

            @Override public int getLoginTimeout() {
                return 0;
            }

            @Override public java.util.logging.Logger getParentLogger() {
                return null;
            }

            @Override public <T> T unwrap(final Class<T> iface) {
                return null;
            }

            @Override public boolean isWrapperFor(final Class<?> iface) {
                return false;
            }
        };
    }

    @Test
    @Order(1)
    @DisplayName("Pool starts and provides a valid connection")
    void givenPooledDataSource_whenGetConnection_returnValidConnection() throws SQLException {
        try (final Connection conn = this.hikariDS.getConnection()) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
            assertTrue(conn.isValid(2));
        }
    }

    @Test
    @Order(2)
    @DisplayName("INSERT and SELECT round-trip through pool")
    void givenPooledDataSource_whenInsertData_persistDataAsExpected() throws SQLException {
        try (
            final Connection conn = hikariDS.getConnection();
            final PreparedStatement insertStmt = conn.prepareStatement(
                "INSERT INTO pooled_test (somekey, someInt) VALUES (?, ?)")
        ) {
            insertStmt.setString(1, "price");
            insertStmt.setInt(2, 100);
            insertStmt.executeUpdate();
        }

        try (
            final Connection conn = hikariDS.getConnection();
            final PreparedStatement select = conn.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?")
        ) {

            select.setString(1, "price");
            try (final ResultSet rs = select.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt("someInt"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("Connections are reused rather than recreated")
    void givenPooledDataSource_whenGetConnection_reuseThemIfPossible() throws SQLException {
        // This should not grow the pool beyond POOL_MAX_SIZE = 20.
        for (int i = 0; i < 50; i++) {
            try (final Connection conn = this.hikariDS.getConnection()) {
                assertFalse(conn.isClosed());
            }
        }
        // If connections were not reused, activeConnections would spike.
        assertEquals(0, this.hikariDS.getHikariPoolMXBean().getActiveConnections());
    }

    @Test
    @Order(4)
    @DisplayName("Concurrent threads can borrow and use connections without conflicts")
    void givenPooledDataSource_whenInsertDataConcurrently_persistDataAsExpected() throws InterruptedException {
        final int threadCount = 10;
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);

        final ExecutorService executor = newFixedThreadPool(threadCount);
        final CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int id = i;
            executor.submit(() -> {
                try (
                    final Connection conn = this.hikariDS.getConnection();
                    final PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO pooled_test (somekey, someInt) VALUES (?, ?)")
                ) {
                    ps.setString(1, "Thread-" + id);
                    ps.setInt(2, id);
                    ps.executeUpdate();
                    successCount.incrementAndGet();
                } catch (final SQLException e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // All threads should have finished successfully
        assertEquals(0, errorCount.get());
        assertEquals(threadCount, successCount.get());

        // Check all threads have inserted a row in the table.
        try (
            final Connection conn = this.hikariDS.getConnection();
            final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery("SELECT COUNT(somekey) FROM pooled_test")
        ) {
            rs.next();
            assertEquals(threadCount, rs.getInt(1));
        } catch (final SQLException e) {
            fail("Failed to count inserted rows: " + e.getMessage());
        }
    }

    @Test
    @Order(5)
    @DisplayName("Acquiring more connections than pool max triggers timeout")
    void givenPooledDataSource_whenPoolMaxSizeExceeded_throwTimeoutException() throws SQLException {
        final List<Connection> heldConnections = new ArrayList<>();
        try {
            for (int i = 0; i < POOL_MAX_SIZE; i++) {
                heldConnections.add(this.hikariDS.getConnection());
            }
            // Any additional connection should trigger a connection timeout.
            assertThrows(SQLException.class, hikariDS::getConnection);
        } finally {
            for (final Connection conn : heldConnections) {
                try {
                    conn.close();
                } catch (final SQLException e) {
                    // ignore exceptions here
                }
            }
        }
    }

    @Test
    @Order(6)
    @DisplayName("Batch inserts execute correctly through pool")
    void givenPooledDataSource_whenBatchInsertData_persistDataAsExpected() throws SQLException {
        final int batchSize = 50;

        try (
            final Connection conn = this.hikariDS.getConnection();
            final PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO pooled_test (somekey, someInt) VALUES (?, ?)")
        ) {
            for (int i = 0; i < batchSize; i++) {
                ps.setString(1, "Batch-" + i);
                ps.setInt(2, 99);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            conn.commit();

            assertEquals(batchSize, results.length);
            for (final int result : results) {
                assertTrue(result >= 1 || result == Statement.SUCCESS_NO_INFO);
            }
        }

        try (
            final Connection conn = this.hikariDS.getConnection();
            final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(somekey) FROM pooled_test WHERE someInt = 99 ALLOW FILTERING")
        ) {
            rs.next();
            assertEquals(batchSize, rs.getInt(1));
        }
    }

    @Test
    @Order(7)
    @DisplayName("Pool remains healthy after several borrow-return cycles")
    void testPoolHealthAfterCycles() throws SQLException {
        for (int cycle = 0; cycle < 30; cycle++) {
            try (
                final Connection conn = this.hikariDS.getConnection();
                final Statement stmt = conn.createStatement()
            ) {
                stmt.executeQuery("SELECT key FROM system.local");
            }
        }

        try (final Connection conn = this.hikariDS.getConnection()) {
            assertTrue(conn.isValid(2));
        }
        assertEquals(0, hikariDS.getHikariPoolMXBean().getActiveConnections());
    }

    @Test
    @Order(8)
    @DisplayName("Build pooled connection using builder")
    void givenDataSource_whenBuildPooledConnectionBuilder_returnCassandraPooledConnection() throws Exception {
        final CassandraPooledConnectionBuilder connectionBuilder =
            (CassandraPooledConnectionBuilder) sut.createPooledConnectionBuilder();
        final PooledCassandraConnection connection = (PooledCassandraConnection) connectionBuilder
            .user("testUser")
            .password("testPassword")
            .contactPoints(CONTACT_POINTS)
            .databaseName(KEYSPACE)
            .consistency("TWO")
            .serialConsistency("LOCAL_SERIAL")
            .fetchSize(5_000)
            .localDataCenter("DC1")
            .loadBalancingPolicy("com.ing.data.cassandra.jdbc.testing.AnotherFakeLoadBalancingPolicy")
            .requestTimeout(8_000L)
            .retryPolicy("com.ing.data.cassandra.jdbc.testing.AnotherFakeRetryPolicy")
            .reconnectionPolicy("ConstantReconnectionPolicy((long)10)")
            .connectionTimeout(15_000L)
            .tcpNoDelayEnabled(false)
            .tcpKeepAliveEnabled(true)
            .build();
        assertConnectionHasExpectedConfig(connection.getConnection(), KEYSPACE);
        connection.close();
    }
}
