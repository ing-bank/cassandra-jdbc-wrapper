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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.Utils.NOT_SUPPORTED;

/**
 * Pooled Cassandra data source: implementation class for {@link DataSource} and {@link ConnectionEventListener}.
 */
public class PooledCassandraDataSource implements DataSource, ConnectionEventListener {

    private static final int CONNECTION_IS_VALID_DEFAULT_TIMEOUT = 5;
    private static final int MIN_POOL_SIZE = 4;
    private static final Logger log = LoggerFactory.getLogger(PooledCassandraDataSource.class);

    private final CassandraDataSource connectionPoolDataSource;
    private final Set<PooledCassandraConnection> freeConnections = new HashSet<>();
    private final Set<PooledCassandraConnection> usedConnections = new HashSet<>();

    /**
     * Constructor.
     *
     * @param connectionPoolDataSource The underlying {@link CassandraDataSource}.
     */
    public PooledCassandraDataSource(final CassandraDataSource connectionPoolDataSource) {
        this.connectionPoolDataSource = connectionPoolDataSource;
    }

    @Override
    public synchronized Connection getConnection() throws SQLException {
        final PooledCassandraConnection pooledConnection;
        if (this.freeConnections.isEmpty()) {
            pooledConnection = this.connectionPoolDataSource.getPooledConnection();
            pooledConnection.addConnectionEventListener(this);
        } else {
            pooledConnection = this.freeConnections.iterator().next();
            this.freeConnections.remove(pooledConnection);
        }
        this.usedConnections.add(pooledConnection);
        return new ManagedConnection(pooledConnection);
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
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
    public boolean isWrapperFor(final Class<?> arg0) {
        return this.connectionPoolDataSource.isWrapperFor(arg0);
    }

    @Override
    public <T> T unwrap(final Class<T> arg0) throws SQLException {
        return connectionPoolDataSource.unwrap(arg0);
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.connectionPoolDataSource.getParentLogger();
    }
}
