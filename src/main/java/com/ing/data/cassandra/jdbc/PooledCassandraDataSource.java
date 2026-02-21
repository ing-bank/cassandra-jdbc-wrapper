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

package com.ing.data.cassandra.jdbc;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.PooledConnectionBuilder;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKeyBuilder;
import java.util.HashSet;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;

/**
 * Pooled Cassandra data source: implementation class for {@link ConnectionPoolDataSource} and
 * {@link ConnectionEventListener}.
 */
@Slf4j
public class PooledCassandraDataSource implements DataSource, ConnectionPoolDataSource, ConnectionEventListener {

    private static final int CONNECTION_IS_VALID_DEFAULT_TIMEOUT = 5;
    private static final int MIN_POOL_SIZE = 4;

    private final CassandraDataSource connectionPoolDataSource;
    private final Set<PooledCassandraConnection> freeConnections = new HashSet<>();
    private final Set<PooledCassandraConnection> usedConnections = new HashSet<>();

    /**
     * Constructor.
     *
     * @param connectionPoolDataSource The underlying {@link CassandraDataSource}.
     */
    public PooledCassandraDataSource(@Nonnull final CassandraDataSource connectionPoolDataSource) {
        this.connectionPoolDataSource = connectionPoolDataSource;
    }

    @Override
    public synchronized PooledConnection getPooledConnection() throws SQLException {
        return getPooledConnection(
            this.connectionPoolDataSource.getUser(),
            this.connectionPoolDataSource.getPassword()
        );
    }

    @Override
    public PooledConnection getPooledConnection(final String user, final String password) throws SQLException {
        final PooledCassandraConnection pooledConnection;
        if (this.freeConnections.isEmpty()) {
            pooledConnection = this.connectionPoolDataSource.getPooledConnection(user, password);
            pooledConnection.addConnectionEventListener(this);
        } else {
            pooledConnection = this.freeConnections.iterator().next();
            this.freeConnections.remove(pooledConnection);
        }
        this.usedConnections.add(pooledConnection);
        return pooledConnection;
    }

    @Override
    public synchronized Connection getConnection() throws SQLException {
        return getConnection(this.connectionPoolDataSource.getUser(), this.connectionPoolDataSource.getPassword());
    }

    @Override
    public Connection getConnection(final String user, final String password) throws SQLException {
        return new ManagedConnection((PooledCassandraConnection) getPooledConnection(user, password));
    }

    @Override
    public synchronized void connectionClosed(final ConnectionEvent event) {
        final PooledCassandraConnection connection = (PooledCassandraConnection) event.getSource();
        this.usedConnections.remove(connection);
        final int freeConnectionsCount = this.freeConnections.size();
        if (freeConnectionsCount < MIN_POOL_SIZE) {
            this.freeConnections.add(connection);
        } else {
            try {
                connection.close();
            } catch (final SQLException e) {
                log.error(e.getMessage());
            }
        }
    }

    @Override
    public synchronized void connectionErrorOccurred(final ConnectionEvent event) {
        final PooledCassandraConnection connection = (PooledCassandraConnection) event.getSource();
        try {
            if (!connection.getConnection().isValid(CONNECTION_IS_VALID_DEFAULT_TIMEOUT)) {
                connection.getConnection().close();
            }
        } catch (final SQLException e) {
            log.error(e.getMessage());
        }
        this.usedConnections.remove(connection);
    }

    /**
     * Closes all the pooled connections.
     */
    public synchronized void close() {
        closePooledConnections(this.usedConnections);
        closePooledConnections(this.freeConnections);
    }

    private void closePooledConnections(final Set<PooledCassandraConnection> usedConnections) {
        for (final PooledConnection connection : usedConnections) {
            try {
                connection.close();
            } catch (final SQLException e) {
                log.error(e.getMessage());
            }
        }
    }

    @Override
    public PooledConnectionBuilder createPooledConnectionBuilder() throws SQLException {
        // todo
        return null;
    }

    @Override
    public int getLoginTimeout() {
        return this.connectionPoolDataSource.getLoginTimeout();
    }

    @Override
    public void setLoginTimeout(final int seconds) {
        this.connectionPoolDataSource.setLoginTimeout(seconds);
    }

    @Override
    public PrintWriter getLogWriter() {
        return this.connectionPoolDataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(final PrintWriter writer) {
        this.connectionPoolDataSource.setLogWriter(writer);
    }

    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return this.connectionPoolDataSource.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return this.connectionPoolDataSource.unwrap(iface);
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.connectionPoolDataSource.getParentLogger();
    }
}
